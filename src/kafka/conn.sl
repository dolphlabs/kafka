// One broker connection: dial, TLS, ApiVersions, SASL, then requests
// pipelined over it and a reader task that hands each response to the
// request it answers.
//
// A broker answers a connection's requests in the order they were sent,
// so the writer queues each request's Call before writing it (both under
// the write lock), and the reader pops them in the same order. A request
// past its deadline closes the connection, as the Java client does: the
// response may still arrive, and the stream cannot be trusted to line up
// with what was abandoned.

import "net";
import "strings";
import "time";

let MS = 1000000;
let READ_CHUNK = 65536;

pub gc struct Sasl {
    // "" (none), "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER"
    mechanism: str,
    user: str,
    pass: str,
    token: str,
}

pub fn no_sasl() -> Sasl {
    return Sasl { mechanism: "", user: "", pass: "", token: "" };
}

pub fn plain(user: str, pass: str) -> Sasl {
    return Sasl { mechanism: "PLAIN", user: user, pass: pass, token: "" };
}

pub fn scram_sha256(user: str, pass: str) -> Sasl {
    return Sasl { mechanism: "SCRAM-SHA-256", user: user, pass: pass, token: "" };
}

pub fn scram_sha512(user: str, pass: str) -> Sasl {
    return Sasl { mechanism: "SCRAM-SHA-512", user: user, pass: pass, token: "" };
}

// A bearer token (an OAuth access token, for example from an identity
// provider's client-credentials flow).
pub fn oauthbearer(token: str) -> Sasl {
    return Sasl { mechanism: "OAUTHBEARER", user: "", pass: "", token: token };
}

pub gc struct Config {
    // "host:port" (or "[v6]:port"); any one of them is enough to find the rest
    brokers: [str],
    client_id: str,
    // sent with fetches so a broker can pick a replica in this rack
    rack: str,
    tls: bool,
    // CA file ("" is the system trust store); the host name is verified
    ca_path: str,
    // a client certificate and key, for brokers that require mTLS
    cert_path: str,
    key_path: str,
    sasl: Sasl,
    // SASL PLAIN sends the password as is: refused without TLS unless set
    sasl_plain_without_tls: bool,
    dial_timeout_ms: int,
    // how long a request may wait for its response
    request_timeout_ms: int,
    // metadata older than this is refreshed
    metadata_max_age_ms: int,
    // the largest response read from a broker
    max_response_bytes: int,
    retry_backoff_ms: int,
}

pub fn config(brokers: [str]) -> Config {
    return Config { brokers: brokers, client_id: "slang-kafka", rack: "", tls: false, ca_path: "", cert_path: "",
                    key_path: "", sasl: no_sasl(), sasl_plain_without_tls: false, dial_timeout_ms: 10000,
                    request_timeout_ms: 30000, metadata_max_age_ms: 300000, max_response_bytes: 104857600,
                    retry_backoff_ms: 100 };
}

// ---- deadlines ------------------------------------------------------------------

// Woken at the first of two deadlines (the caller's and a timeout of
// the client's own; until values cannot be compared, so both are kept).
gc struct Waiter {
    deadline: until,
    cap: until,
    wake: chan[bool],
    done: bool,
}

// A task that wakes waiters at their deadlines, shared by a client's
// connections. Parked while nothing waits.
gc struct Clock {
    lock: mutex,
    waiters: [Waiter],
    tick: chan[bool],
    stopped: bool,
}

fn new_clock() -> Clock {
    let c = Clock { lock: make_mutex(), waiters: [], tick: make_chan(1), stopped: false };
    spawn clock_loop(c);
    return c;
}

fn clock_loop(c: Clock) {
    while !c.stopped {
        mutex_lock(c.lock);
        let any = len(c.waiters) > 0;
        mutex_unlock(c.lock);
        if !any {
            let t = chan_recv(c.tick);
            if let _v = t {
                continue;
            }
            break;
        }
        time.sleep(2 * MS);
        mutex_lock(c.lock);
        let keep: [Waiter] = [];
        for w in c.waiters {
            if w.done {
                continue;
            }
            if until_hit(w.deadline) || until_hit(w.cap) {
                w.done = true;
                select {
                    case chan_send(w.wake, true) { }
                    default { }
                }
            } else {
                push(keep, w);
            }
        }
        c.waiters = keep;
        mutex_unlock(c.lock);
    }
}

fn add_waiter(c: Clock, deadline: until, cap: until) -> Waiter {
    let w = Waiter { deadline: deadline, cap: cap, wake: make_chan(1), done: false };
    mutex_lock(c.lock);
    push(c.waiters, w);
    mutex_unlock(c.lock);
    select {
        case chan_send(c.tick, true) { }
        default { }
    }
    return w;
}

fn stop_clock(c: Clock) {
    if c.stopped {
        return;
    }
    c.stopped = true;
    mutex_lock(c.lock);
    let ws = c.waiters;
    c.waiters = [];
    mutex_unlock(c.lock);
    for w in ws {
        select {
            case chan_send(w.wake, true) { }
            default { }
        }
    }
    chan_close(c.tick);
}

// A deadline ms milliseconds from now.
pub fn after_ms(ms: int) -> until {
    return until_of(time.mono() + ms * MS);
}

// ---- connections ------------------------------------------------------------------

gc struct Call {
    key: int,
    corr: int,
    done: chan[bool],
    finished: bool,
    resp: bytes,
    err: str,
}

pub gc struct Conn {
    cfg: Config,
    clock: Clock,
    node: int,
    host: str,
    port: int,
    fd: int,
    ssl: rawptr,
    tls_ctx: rawptr,
    closed: bool,
    why: str,
    wlock: mutex,
    plock: mutex,
    pending: [Call],
    head: int,
    corr: int,
    apis: map[int]ApiRange,
    // bytes read past the last frame
    rb: bytes,
}

// Splits "host:port" and "[v6]:port".
pub fn split_host_port(s: str) -> result[Target, str] {
    let host = s;
    let port_s = "";
    if strings.has_prefix(s, "[") {
        let close = strings.find(s, "]");
        if close < 0 {
            return err("kafka: broker address " + s + " has no closing ]");
        }
        host = strings.slice(s, 1, close);
        let rest = strings.slice(s, close + 1, len(s));
        if !strings.has_prefix(rest, ":") {
            return err("kafka: broker address " + s + " has no port");
        }
        port_s = strings.slice(rest, 1, len(rest));
    } else {
        let colon = strings.rfind(s, ":");
        if colon < 0 {
            return err("kafka: broker address " + s + " has no port");
        }
        host = strings.slice(s, 0, colon);
        port_s = strings.slice(s, colon + 1, len(s));
    }
    let pr = to_int(port_s);
    guard let port = pr else { return err("kafka: broker address " + s + " has a bad port"); }
    if port < 1 || port > 65535 || host == "" {
        return err("kafka: broker address " + s + " is not host:port");
    }
    return ok(Target { host: host, port: port });
}

pub gc struct Target {
    host: str,
    port: int,
}

// Dials host:port, upgrades to TLS, checks the broker's API versions and
// authenticates, all before deadline.
fn open_conn(cfg: Config, clock: Clock, node: int, host: str, port: int, deadline: until) -> result[Conn, str] {
    let c = Conn { cfg: cfg, clock: clock, node: node, host: host, port: port, fd: -1, ssl: nullptr, tls_ctx: nullptr,
                   closed: false, why: "", wlock: make_mutex(), plock: make_mutex(), pending: [], head: 0, corr: 0,
                   apis: {}, rb: b"" };
    if until_hit(deadline) {
        return err("kafka: dial " + host + ":" + to_str(port) + ": the deadline passed");
    }
    let dr = net.dial_until(host, port, after_ms(cfg.dial_timeout_ms));
    guard let fd = dr else let e = err_of(dr) {
        return err("kafka: dial " + host + ":" + to_str(port) + ": " + e);
    }
    c.fd = fd as int;
    let hr = handshake(c, deadline);
    guard let _h = hr else let e = err_of(hr) {
        close_transport(c);
        c.closed = true;
        return err(e);
    }
    spawn read_loop(c);
    return ok(c);
}

fn handshake(c: Conn, deadline: until) -> result[bool, str] {
    if c.cfg.tls {
        let cr = net.tls_client_ctx(c.cfg.ca_path);
        guard let ctx = cr else let e = err_of(cr) { return err("kafka: tls: " + e); }
        if c.cfg.cert_path != "" {
            let kr = net.tls_ctx_use_cert(ctx, c.cfg.cert_path, c.cfg.key_path);
            guard let _k = kr else let e = err_of(kr) { return err("kafka: tls: client certificate: " + e); }
        }
        c.tls_ctx = ctx;
        let ur = net.tls_upgrade_until(c.fd, c.host, ctx, deadline);
        guard let ssl = ur else let e = err_of(ur) { return err("kafka: tls " + c.host + ": " + e); }
        c.ssl = ssl;
    }
    let body = new_buf();
    enc_api_versions(body, "slang-kafka", "0.1.0");
    let ar = sync_call(c, API_API_VERSIONS, body, deadline);
    guard let r = ar else let e = err_of(ar) { return err(e); }
    let vr = dec_api_versions(r);
    guard let v = vr else let e = err_of(vr) { return err(e); }
    if v.error_code == ERR_UNSUPPORTED_VERSION {
        return err("kafka: broker " + c.host + ":" + to_str(c.port) +
                   " is older than Kafka 2.4 (no ApiVersions v3); this client needs Kafka 3.0 or later");
    }
    if v.error_code != 0 {
        return err(kerr("ApiVersions to " + c.host, v.error_code, ""));
    }
    for a in v.apis {
        c.apis[a.key] = a;
    }
    // the calls every connection needs; the rest are checked when used
    for key in [API_METADATA, API_PRODUCE, API_FETCH] {
        let sr = check_api(c, key);
        guard let _s = sr else let e = err_of(sr) { return err(e); }
    }
    if c.cfg.sasl.mechanism != "" {
        let sr = authenticate(c, deadline);
        guard let _s = sr else let e = err_of(sr) { return err(e); }
    }
    return ok(true);
}

// An error when the broker does not support the version this client
// sends for key.
pub fn check_api(c: Conn, key: int) -> result[bool, str] {
    let want = api_version(key);
    if !has(c.apis, key) {
        return err("kafka: broker " + label(c) + " does not support " + api_name(key));
    }
    let a = c.apis[key];
    if want < a.min || want > a.max {
        return err("kafka: broker " + label(c) + " supports " + api_name(key) + " v" +
                   to_str(a.min) + "-" + to_str(a.max) + "; this client sends v" + to_str(want) +
                   " (Kafka 3.0 or later is needed)");
    }
    return ok(true);
}

// A request and its response before the reader runs (the handshake).
fn sync_call(c: Conn, key: int, body: Buf, deadline: until) -> result[Reader, str] {
    c.corr += 1;
    let frame = frame_request(key, c.corr, c.cfg.client_id, body);
    let wr = write_all(c, frame.finish(), deadline);
    guard let _w = wr else let e = err_of(wr) { return err(e); }
    let fr = read_frame(c, deadline);
    guard let f = fr else let e = err_of(fr) { return err(e); }
    return response_reader(key, f, c.corr);
}

// ---- I/O ----------------------------------------------------------------------------

fn net_recv(c: Conn, n: int, deadline: until) -> result[bytes, str] {
    if c.ssl != nullptr {
        return net.tls_recv_until(c.ssl, n, deadline);
    }
    return net.recv_until(c.fd, n, deadline);
}

fn net_send(c: Conn, b: bytes, deadline: until) -> result[i32, str] {
    if c.ssl != nullptr {
        return net.tls_send_until(c.ssl, b, deadline);
    }
    return net.send_until(c.fd, b, deadline);
}

fn write_all(c: Conn, data: bytes, deadline: until) -> result[bool, str] {
    let off = 0;
    while off < len(data) {
        let chunk = data;
        if off > 0 {
            chunk = data[off..];
        }
        let r = net_send(c, chunk, deadline);
        guard let n = r else let e = err_of(r) { return err("kafka: write to broker " + label(c) + ": " + e); }
        if n <= 0 {
            return err("kafka: write to broker " + label(c) + ": the connection is closed");
        }
        off += n as int;
    }
    return ok(true);
}

// The next response frame (without its size prefix). A frame over
// max_response_bytes is refused before it is read.
fn read_frame(c: Conn, deadline: until) -> result[bytes, str] {
    while len(c.rb) < 4 {
        let mr = more(c, READ_CHUNK, deadline);
        guard let _m = mr else let e = err_of(mr) { return err(e); }
    }
    let b = c.rb;
    let size = (b[0] << 24) | (b[1] << 16) | (b[2] << 8) | b[3];
    if size >= 2147483648 || size < 4 {
        return err("kafka: broker " + label(c) + " sent a response of " + to_str(size) + " bytes");
    }
    if size > c.cfg.max_response_bytes {
        return err("kafka: broker " + label(c) + " sent a " + to_str(size) +
                   "-byte response, over max_response_bytes (" + to_str(c.cfg.max_response_bytes) + ")");
    }
    let have = len(c.rb) - 4;
    if have >= size {
        let frame = c.rb[4..4 + size];
        c.rb = c.rb[4 + size..];
        return ok(frame);
    }
    // collect the rest as parts and join them once
    let parts: [bytes] = [c.rb[4..]];
    c.rb = b"";
    while have < size {
        let want = size - have;
        if want < READ_CHUNK {
            want = READ_CHUNK;
        }
        let rr = net_recv(c, want, deadline);
        guard let got = rr else let e = err_of(rr) { return err(read_error(c, e)); }
        if len(got) == 0 {
            return err("kafka: broker " + label(c) + " closed the connection");
        }
        if have + len(got) > size {
            let used = size - have;
            push(parts, got[0..used]);
            c.rb = got[used..];
            have = size;
        } else {
            push(parts, got);
            have += len(got);
        }
    }
    return ok(strings.join_bytes(parts, b""));
}

fn more(c: Conn, n: int, deadline: until) -> result[bool, str] {
    let rr = net_recv(c, n, deadline);
    guard let got = rr else let e = err_of(rr) { return err(read_error(c, e)); }
    if len(got) == 0 {
        return err("kafka: broker " + label(c) + " closed the connection");
    }
    if len(c.rb) == 0 {
        c.rb = got;
    } else {
        c.rb = c.rb + got;
    }
    return ok(true);
}

fn read_error(c: Conn, e: str) -> str {
    if e == "timeout" {
        return "kafka: timed out waiting for broker " + label(c);
    }
    return "kafka: read from broker " + label(c) + ": " + e;
}

// ---- requests ---------------------------------------------------------------------

// Sends a request and waits for its response, until deadline (and at
// most the configured request timeout).
pub fn call(c: Conn, key: int, body: Buf, deadline: until) -> result[Reader, str] {
    return call_timeout(c, key, body, deadline, c.cfg.request_timeout_ms);
}

// call, with a request timeout of timeout_ms instead of the config's (a
// JoinGroup may wait for the whole rebalance).
pub fn call_timeout(c: Conn, key: int, body: Buf, deadline: until, timeout_ms: int) -> result[Reader, str] {
    let sr = send_request(c, key, body, true);
    guard let call = sr else let e = err_of(sr) { return err(e); }
    let fr = await_call(c, call, deadline, timeout_ms);
    guard let frame = fr else let e = err_of(fr) { return err(e); }
    return response_reader(key, frame, call.corr);
}

// Queues and writes a request. expect_reply false: no response comes
// (a produce with acks=0).
fn send_request(c: Conn, key: int, body: Buf, expect_reply: bool) -> result[Call, str] {
    let call = Call { key: key, corr: 0, done: make_chan(1), finished: false, resp: b"", err: "" };
    mutex_lock(c.wlock);
    if c.closed {
        mutex_unlock(c.wlock);
        return err(closed_error(c));
    }
    c.corr += 1;
    call.corr = c.corr;
    let frame = frame_request(key, call.corr, c.cfg.client_id, body);
    if expect_reply {
        mutex_lock(c.plock);
        push(c.pending, call);
        mutex_unlock(c.plock);
    }
    let wr = write_all(c, frame.finish(), after_ms(c.cfg.request_timeout_ms));
    mutex_unlock(c.wlock);
    guard let _w = wr else let e = err_of(wr) {
        shut_down(c, e);
        return err(e);
    }
    return ok(call);
}

fn await_call(c: Conn, call: Call, deadline: until, timeout_ms: int) -> result[bytes, str] {
    let capped = false;
    if !call.finished {
        let w = add_waiter(c.clock, deadline, after_ms(timeout_ms));
        select {
            case let _d = chan_recv(call.done) { }
            case let _t = chan_recv(w.wake) { }
        }
        w.done = true;
        capped = until_hit(w.cap);
    }
    if call.finished {
        if call.err != "" {
            return err(call.err);
        }
        return ok(call.resp);
    }
    let why = "kafka: " + api_name(call.key) + " to broker " + label(c) + " timed out";
    if capped {
        // the broker is not answering: nothing behind this call can be
        // trusted to arrive either
        shut_down(c, why);
    }
    // otherwise only the caller gave up: the response still arrives in
    // order, and the reader drops it
    return err(why);
}

fn read_loop(c: Conn) {
    while !c.closed {
        let fr = read_frame(c, until_never());
        guard let frame = fr else let e = err_of(fr) {
            shut_down(c, e);
            break;
        }
        mutex_lock(c.plock);
        if c.head >= len(c.pending) {
            mutex_unlock(c.plock);
            shut_down(c, "kafka: broker " + label(c) + " sent a response to no request");
            break;
        }
        let call = c.pending[c.head];
        c.head += 1;
        if c.head > 64 && c.head * 2 > len(c.pending) {
            let rest: [Call] = [];
            for i in c.head..len(c.pending) {
                push(rest, c.pending[i]);
            }
            c.pending = rest;
            c.head = 0;
        }
        mutex_unlock(c.plock);
        finish_call(call, frame, "");
    }
}

fn finish_call(call: Call, resp: bytes, e: str) {
    if call.finished {
        return;
    }
    call.resp = resp;
    call.err = e;
    call.finished = true;
    select {
        case chan_send(call.done, true) { }
        default { }
    }
}

// ---- closing ------------------------------------------------------------------------

fn shut_down(c: Conn, why: str) {
    mutex_lock(c.plock);
    if c.closed {
        mutex_unlock(c.plock);
        return;
    }
    c.closed = true;
    if c.why == "" {
        c.why = why;
    }
    let calls: [Call] = [];
    for i in c.head..len(c.pending) {
        push(calls, c.pending[i]);
    }
    c.pending = [];
    c.head = 0;
    mutex_unlock(c.plock);
    close_transport(c);
    for call in calls {
        finish_call(call, b"", why);
    }
}

fn close_transport(c: Conn) {
    if c.ssl != nullptr {
        net.tls_close(c.ssl);
        c.ssl = nullptr;
    }
    if c.fd >= 0 {
        net.close(c.fd);
        c.fd = -1;
    }
}

// "3" for a known broker, its address for a seed.
fn label(c: Conn) -> str {
    if c.node < 0 {
        return c.host + ":" + to_str(c.port);
    }
    return to_str(c.node);
}

fn closed_error(c: Conn) -> str {
    if c.why != "" {
        return c.why;
    }
    return "kafka: the connection to broker " + label(c) + " is closed";
}

pub fn close_conn(c: Conn) {
    shut_down(c, "kafka: the connection was closed");
}

pub fn conn_closed(c: Conn) -> bool {
    return c.closed;
}
