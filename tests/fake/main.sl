// The connection against scripted brokers that misbehave: each scenario
// is a task on an ephemeral port playing the broker's side with the
// package's own writers. Run with tests/run.sh.

import "kafka";
import "net";
import "strings";
import "time";

gc struct Srv {
    lfd: int,
    port: int,
    fd: int,
    data: bytes,
}

fn listen() -> Srv {
    let lr = net.listen(0);
    guard let lfd = lr else let e = err_of(lr) { panic(e); }
    let pr = net.port(lfd);
    guard let port = pr else let e = err_of(pr) { panic(e); }
    return Srv { lfd: lfd as int, port: port as int, fd: -1, data: b"" };
}

gc struct Req {
    key: int,
    corr: int,
}

// The next request's api key and correlation id; none when the client went away.
fn read_req(s: Srv) -> opt[Req] {
    while true {
        if len(s.data) >= 4 {
            let size = (s.data[0] << 24) | (s.data[1] << 16) | (s.data[2] << 8) | s.data[3];
            if len(s.data) >= 4 + size {
                let r = kafka.reader(s.data, 4, 4 + size);
                let key = kafka.rd_i16(r);
                kafka.rd_i16(r);
                let corr = kafka.rd_i32(r);
                s.data = s.data[4 + size..];
                return some(Req { key: key, corr: corr });
            }
        }
        let rr = net.recv_until(s.fd, 65536, kafka.after_ms(3000));
        guard let b = rr else { return none; }
        if len(b) == 0 {
            return none;
        }
        s.data = s.data + b;
    }
    return none;
}

fn send(s: Srv, b: bytes) {
    net.send_until(s.fd, b, kafka.after_ms(3000));
}

// A response frame: size, correlation id, header tags unless ApiVersions.
fn respond(s: Srv, q: Req, body: kafka.Buf) {
    let w = kafka.new_buf();
    let extra = 4;
    if q.key != kafka.API_API_VERSIONS {
        extra = 5;
    }
    kafka.wr_i32(w, extra + body.size());
    kafka.wr_i32(w, q.corr);
    if q.key != kafka.API_API_VERSIONS {
        kafka.wr_tags(w);
    }
    kafka.wr_buf(w, body);
    send(s, w.finish());
}

fn versions_body(produce_max: int) -> kafka.Buf {
    let keys = [0, 1, 2, 3, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 24, 25, 26, 28, 32, 36, 37, 42, 44];
    let b = kafka.new_buf();
    kafka.wr_i16(b, 0);
    kafka.wr_clen(b, len(keys));
    for k in keys {
        kafka.wr_i16(b, k);
        kafka.wr_i16(b, 0);
        if k == kafka.API_PRODUCE {
            kafka.wr_i16(b, produce_max);
        } else {
            kafka.wr_i16(b, kafka.api_version(k) + 2);
        }
        kafka.wr_tags(b);
    }
    kafka.wr_i32(b, 0);
    kafka.wr_tags(b);
    return b;
}

fn serve(s: Srv, scenario: str) {
    let ar = net.accept(s.lfd);
    guard let fd = ar else { return; }
    s.fd = fd as int;
    let qr = read_req(s);
    guard let q = qr else { net.close(s.fd); return; }
    if scenario == "old broker" {
        let b = kafka.new_buf();
        kafka.wr_i16(b, kafka.ERR_UNSUPPORTED_VERSION);
        kafka.wr_i32(b, 1);
        kafka.wr_i16(b, 18);
        kafka.wr_i16(b, 0);
        kafka.wr_i16(b, 2);
        respond(s, q, b);
    } else if scenario == "produce too old" {
        respond(s, q, versions_body(7));
    } else if scenario == "oversized response" {
        send(s, b"\x0c\x00\x00\x00");
    } else if scenario == "negative size" {
        send(s, b"\xff\xff\xff\xf0");
    } else if scenario == "wrong correlation id" {
        respond(s, Req { key: q.key, corr: q.corr + 1 }, versions_body(9));
    } else if scenario == "silent" {
        time.sleep(1500000000);
    } else if scenario == "cut mid-response" {
        send(s, b"\x00\x00\x00\x64\x00\x00\x00\x01");
    } else {
        respond(s, q, versions_body(9));
        let mr = read_req(s);
        guard let m = mr else { net.close(s.fd); return; }
        if scenario == "garbage metadata" {
            let b = kafka.new_buf();
            kafka.wr_i32(b, 0);
            kafka.wr_uvarint(b, 1000);
            respond(s, m, b);
        } else {
            // "metadata never answered"
            time.sleep(1500000000);
        }
    }
    time.sleep(200000000);
    net.close(s.fd);
}

fn run(scenario: str) {
    let s = listen();
    spawn serve(s, scenario);
    let cfg = kafka.config(["127.0.0.1:" + to_str(s.port)]);
    cfg.request_timeout_ms = 400;
    let r = kafka.connect(cfg, kafka.after_ms(600));
    if let cl = r {
        println(scenario + ": connected (wrong)");
        kafka.close(cl);
    } else let e = err_of(r) {
        println(scenario + ": " + strings.replace(e, "127.0.0.1:" + to_str(s.port), "127.0.0.1:PORT"));
    }
    time.sleep(250000000);
    net.close(s.lfd);
}

for sc in ["old broker", "produce too old", "oversized response", "negative size", "wrong correlation id",
           "silent", "cut mid-response", "metadata never answered", "garbage metadata"] {
    run(sc);
}
