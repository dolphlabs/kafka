// The cluster client: bootstrap from any broker, a metadata cache
// (brokers, topics, partition leaders) refreshed when it ages or when a
// broker says it is stale, connections per broker and purpose, and
// coordinator lookup.
//
//     let clr = kafka.connect(kafka.config(["kafka-1:9092"]), deadline);
//     guard let cl = clr else let e = err_of(clr) { panic(e); }
//     ...
//     kafka.close(cl);
//
// A connection carries one purpose: "ctl" (metadata, admin), "produce",
// "fetch", or "group:<id>". A broker answers a connection's requests in
// order, so a fetch that long-polls for half a second would otherwise
// hold up a produce or a heartbeat queued behind it.

import "strings";
import "time";

pub gc struct TopicInfo {
    name: str,
    error_code: int,
    internal: bool,
    // indexed by partition id
    partitions: [PartitionMeta],
}

pub gc struct Client {
    cfg: Config,
    crc: Crc,
    clock: Clock,
    lock: mutex,
    seeds: [Target],
    brokers: map[int]Broker,
    conns: map[str]Conn,
    topics: map[str]TopicInfo,
    // topics a producer or consumer uses: kept in the metadata
    interest: map[str]bool,
    cluster_id: str,
    controller: int,
    meta_ns: int,
    meta_lock: mutex,
    meta_stale: bool,
    coords: map[str]int,
    closed: bool,
}

// Connects to the first broker in cfg.brokers that answers, and learns
// the cluster's brokers from it.
pub fn connect(cfg: Config, deadline: until) -> result[Client, str] {
    if len(cfg.brokers) == 0 {
        return err("kafka: no brokers in the config");
    }
    if cfg.max_response_bytes < 1048576 || cfg.request_timeout_ms <= 0 || cfg.dial_timeout_ms <= 0 {
        return err("kafka: max_response_bytes must be at least 1 MiB and the timeouts positive");
    }
    let seeds: [Target] = [];
    for b in cfg.brokers {
        let tr = split_host_port(b);
        guard let t = tr else let e = err_of(tr) { return err(e); }
        push(seeds, t);
    }
    let cl = Client { cfg: cfg, crc: crc_tables(), clock: new_clock(), lock: make_mutex(), seeds: seeds, brokers: {},
                      conns: {}, topics: {}, interest: {}, cluster_id: "", controller: -1, meta_ns: 0,
                      meta_lock: make_mutex(), meta_stale: false, coords: {}, closed: false };
    let rr = refresh_metadata(cl, deadline);
    guard let _r = rr else let e = err_of(rr) {
        close(cl);
        return err(e);
    }
    spawn meta_loop(cl);
    return ok(cl);
}

// Closes every connection. Producers and consumers made from the client
// should be closed first.
pub fn close(cl: Client) {
    mutex_lock(cl.lock);
    if cl.closed {
        mutex_unlock(cl.lock);
        return;
    }
    cl.closed = true;
    let conns: [Conn] = [];
    for k, c in cl.conns {
        push(conns, c);
    }
    cl.conns = {};
    mutex_unlock(cl.lock);
    for c in conns {
        close_conn(c);
    }
    stop_clock(cl.clock);
}

pub fn cluster_id(cl: Client) -> str {
    return cl.cluster_id;
}

pub fn controller(cl: Client) -> int {
    return cl.controller;
}

// The brokers from the last metadata, by node id.
pub fn brokers(cl: Client) -> [Broker] {
    mutex_lock(cl.lock);
    let out: [Broker] = [];
    for id, b in cl.brokers {
        push(out, b);
    }
    mutex_unlock(cl.lock);
    return out;
}

fn now_ns() -> int {
    return time.mono() as int;
}

fn sleep_ms(ms: int) {
    time.sleep(ms * MS);
}

// ---- connections ------------------------------------------------------------------

// An open connection to broker node for purpose, dialing one if needed.
pub fn conn_to(cl: Client, node: int, purpose: str, deadline: until) -> result[Conn, str] {
    let key = to_str(node) + "/" + purpose;
    mutex_lock(cl.lock);
    if cl.closed {
        mutex_unlock(cl.lock);
        return err("kafka: the client is closed");
    }
    if has(cl.conns, key) {
        let c = cl.conns[key];
        if !c.closed {
            mutex_unlock(cl.lock);
            return ok(c);
        }
        del(cl.conns, key);
    }
    let known = has(cl.brokers, node);
    mutex_unlock(cl.lock);
    if !known {
        let rr = refresh_metadata(cl, deadline);
        guard let _r = rr else let e = err_of(rr) { return err(e); }
        mutex_lock(cl.lock);
        known = has(cl.brokers, node);
        mutex_unlock(cl.lock);
        if !known {
            return err("kafka: broker " + to_str(node) + " is not in the cluster's metadata");
        }
    }
    mutex_lock(cl.lock);
    let b = cl.brokers[node];
    mutex_unlock(cl.lock);
    let cr = open_conn(cl.cfg, cl.clock, node, b.host, b.port, deadline);
    guard let c = cr else let e = err_of(cr) { return err(e); }
    mutex_lock(cl.lock);
    if cl.closed {
        mutex_unlock(cl.lock);
        close_conn(c);
        return err("kafka: the client is closed");
    }
    if has(cl.conns, key) && !cl.conns[key].closed {
        // another task dialed it first
        let other = cl.conns[key];
        mutex_unlock(cl.lock);
        close_conn(c);
        return ok(other);
    }
    cl.conns[key] = c;
    mutex_unlock(cl.lock);
    return ok(c);
}

// A control connection to any broker: an open one if there is one, else
// the known brokers and then the seeds, in turn.
fn any_conn(cl: Client, deadline: until) -> result[Conn, str] {
    mutex_lock(cl.lock);
    if cl.closed {
        mutex_unlock(cl.lock);
        return err("kafka: the client is closed");
    }
    for k, c in cl.conns {
        if !c.closed && (strings_suffix(k, "/ctl")) {
            mutex_unlock(cl.lock);
            return ok(c);
        }
    }
    let nodes: [int] = [];
    for id, b in cl.brokers {
        push(nodes, id);
    }
    mutex_unlock(cl.lock);
    let last = "kafka: no broker could be reached";
    for id in nodes {
        let cr = conn_to(cl, id, "ctl", deadline);
        if let c = cr {
            return ok(c);
        } else let e = err_of(cr) {
            last = e;
        }
        if until_hit(deadline) {
            return err(last);
        }
    }
    for s in cl.seeds {
        let cr = open_conn(cl.cfg, cl.clock, -1, s.host, s.port, deadline);
        if let c = cr {
            mutex_lock(cl.lock);
            if cl.closed {
                mutex_unlock(cl.lock);
                close_conn(c);
                return err("kafka: the client is closed");
            }
            cl.conns["seed:" + s.host + ":" + to_str(s.port) + "/ctl"] = c;
            mutex_unlock(cl.lock);
            return ok(c);
        } else let e = err_of(cr) {
            last = e;
        }
        if until_hit(deadline) {
            break;
        }
    }
    return err(last);
}

fn strings_suffix(s: str, suffix: str) -> bool {
    let n = len(s);
    let k = len(suffix);
    if k > n {
        return false;
    }
    return to_bytes(s)[n - k..n] == to_bytes(suffix);
}

// ---- metadata ---------------------------------------------------------------------

// Refreshes brokers and the topics in use. Concurrent callers share one
// request.
pub fn refresh_metadata(cl: Client, deadline: until) -> result[bool, str] {
    let started = now_ns();
    mutex_lock(cl.meta_lock);
    if cl.meta_ns > started {
        // another task refreshed while this one waited
        mutex_unlock(cl.meta_lock);
        return ok(true);
    }
    let rr = do_refresh(cl, deadline);
    mutex_unlock(cl.meta_lock);
    return rr;
}

fn do_refresh(cl: Client, deadline: until) -> result[bool, str] {
    mutex_lock(cl.lock);
    let names: [str] = [];
    for t, _v in cl.interest {
        push(names, t);
    }
    mutex_unlock(cl.lock);
    let tries = 0;
    let last = "";
    while true {
        let cr = any_conn(cl, deadline);
        if let c = cr {
            let body = new_buf();
            enc_metadata(body, some(names), false);
            let rr = call(c, API_METADATA, body, deadline);
            if let r = rr {
                let mr = dec_metadata(r);
                guard let m = mr else let e = err_of(mr) { return err(e); }
                apply_metadata(cl, m, true);
                return ok(true);
            } else let e = err_of(rr) {
                last = e;
            }
        } else let e = err_of(cr) {
            last = e;
            if !transient(e) {
                return err(e);
            }
        }
        tries += 1;
        if until_hit(deadline) || tries >= 3 {
            return err(last);
        }
        sleep_ms(cl.cfg.retry_backoff_ms);
    }
    return err(last);
}

// Whether a connection error may clear on its own (a refused dial, a
// dropped or silent connection), as opposed to one retrying cannot fix: a
// broker too old, refused credentials, a bad certificate, a malformed
// response.
fn transient(e: str) -> bool {
    return strings.has_prefix(e, "kafka: dial ") || strings.contains(e, "timed out") ||
           strings.contains(e, "closed the connection") || strings.has_prefix(e, "kafka: read from") ||
           strings.has_prefix(e, "kafka: write to") || strings.contains(e, "is closed");
}

fn index_partitions(t: TopicMeta) -> TopicInfo {
    let max = -1;
    for p in t.partitions {
        if p.partition > max {
            max = p.partition;
        }
    }
    let parts: [PartitionMeta] = [];
    for i in 0..(max + 1) {
        push(parts, PartitionMeta { error_code: ERR_LEADER_NOT_AVAILABLE, partition: i, leader: -1, leader_epoch: -1,
                                    replicas: [], isr: [], offline: [] });
    }
    for p in t.partitions {
        if p.partition >= 0 {
            parts[p.partition] = p;
        }
    }
    return TopicInfo { name: t.name, error_code: t.error_code, internal: t.internal, partitions: parts };
}

fn apply_metadata(cl: Client, m: MetadataResp, brokers_too: bool) {
    mutex_lock(cl.lock);
    if brokers_too && len(m.brokers) > 0 {
        let bs: map[int]Broker = {};
        for b in m.brokers {
            bs[b.node_id] = b;
        }
        cl.brokers = bs;
        cl.controller = m.controller;
        cl.cluster_id = m.cluster_id ?? "";
    }
    for t in m.topics {
        if t.error_code == ERR_UNKNOWN_TOPIC_OR_PARTITION || t.error_code == ERR_TOPIC_AUTHORIZATION_FAILED {
            cl.topics[t.name] = TopicInfo { name: t.name, error_code: t.error_code, internal: t.internal, partitions: [] };
            continue;
        }
        cl.topics[t.name] = index_partitions(t);
    }
    cl.meta_ns = now_ns();
    cl.meta_stale = false;
    mutex_unlock(cl.lock);
}

// Keeps topics in the metadata, and refreshes if any is not known yet.
pub fn use_topics(cl: Client, names: [str], deadline: until) -> result[bool, str] {
    mutex_lock(cl.lock);
    let missing = false;
    for n in names {
        cl.interest[n] = true;
        if !has(cl.topics, n) {
            missing = true;
        }
    }
    mutex_unlock(cl.lock);
    if missing {
        return refresh_metadata(cl, deadline);
    }
    return ok(true);
}

// Asks for a refresh soon: a broker said the metadata is stale.
pub fn metadata_stale(cl: Client) {
    cl.meta_stale = true;
}

// The topic's metadata, if the client has it.
pub fn topic_info(cl: Client, topic: str) -> opt[TopicInfo] {
    mutex_lock(cl.lock);
    if !has(cl.topics, topic) {
        mutex_unlock(cl.lock);
        return none;
    }
    let t = cl.topics[topic];
    mutex_unlock(cl.lock);
    return some(t);
}

// The number of partitions (0 when unknown or missing).
pub fn partition_count(cl: Client, topic: str) -> int {
    let ti = topic_info(cl, topic);
    guard let t = ti else { return 0; }
    return len(t.partitions);
}

// The partition's leader (-1 when unknown).
pub fn leader_of(cl: Client, topic: str, partition: int) -> int {
    let ti = topic_info(cl, topic);
    guard let t = ti else { return -1; }
    if partition < 0 || partition >= len(t.partitions) {
        return -1;
    }
    return t.partitions[partition].leader;
}

pub fn leader_epoch_of(cl: Client, topic: str, partition: int) -> int {
    let ti = topic_info(cl, topic);
    guard let t = ti else { return -1; }
    if partition < 0 || partition >= len(t.partitions) {
        return -1;
    }
    return t.partitions[partition].leader_epoch;
}

// Refreshes metadata when it is older than metadata_max_age_ms or marked
// stale (at most every retry backoff), until the client closes.
fn meta_loop(cl: Client) {
    let step = 100;
    if cl.cfg.retry_backoff_ms > step {
        step = cl.cfg.retry_backoff_ms;
    }
    while !cl.closed {
        sleep_ms(step);
        if cl.closed {
            break;
        }
        let age_ms = (now_ns() - cl.meta_ns) / MS;
        if cl.meta_stale || age_ms >= cl.cfg.metadata_max_age_ms {
            refresh_metadata(cl, after_ms(cl.cfg.request_timeout_ms));
        }
    }
}

// ---- coordinators -----------------------------------------------------------------

// The coordinator broker of a group (COORDINATOR_GROUP) or transactional
// id (COORDINATOR_TRANSACTION), retrying while it is loading or moving.
pub fn coordinator(cl: Client, key: str, kind: int, deadline: until) -> result[int, str] {
    let ck = to_str(kind) + ":" + key;
    mutex_lock(cl.lock);
    if has(cl.coords, ck) {
        let node = cl.coords[ck];
        mutex_unlock(cl.lock);
        return ok(node);
    }
    mutex_unlock(cl.lock);
    let last = "";
    while true {
        let cr = any_conn(cl, deadline);
        guard let c = cr else let e = err_of(cr) { return err(e); }
        let body = new_buf();
        enc_find_coordinator(body, key, kind);
        let rr = call(c, API_FIND_COORDINATOR, body, deadline);
        if let r = rr {
            let fr = dec_find_coordinator(r);
            guard let f = fr else let e = err_of(fr) { return err(e); }
            if f.error_code == 0 {
                mutex_lock(cl.lock);
                cl.coords[ck] = f.node_id;
                if !has(cl.brokers, f.node_id) {
                    cl.brokers[f.node_id] = Broker { node_id: f.node_id, host: f.host, port: f.port, rack: none };
                }
                mutex_unlock(cl.lock);
                return ok(f.node_id);
            }
            last = kerr("FindCoordinator " + key, f.error_code, f.error_message);
            if !is_retriable(f.error_code) {
                return err(last);
            }
        } else let e = err_of(rr) {
            last = e;
        }
        if until_hit(deadline) {
            return err(last);
        }
        sleep_ms(cl.cfg.retry_backoff_ms);
    }
    return err(last);
}

// Forgets a coordinator a broker said has moved (NOT_COORDINATOR,
// COORDINATOR_NOT_AVAILABLE).
pub fn forget_coordinator(cl: Client, key: str, kind: int) {
    mutex_lock(cl.lock);
    let ck = to_str(kind) + ":" + key;
    if has(cl.coords, ck) {
        del(cl.coords, ck);
    }
    mutex_unlock(cl.lock);
}

// A request to broker node over a connection for purpose.
pub fn call_node(cl: Client, node: int, purpose: str, key: int, body: Buf, deadline: until) -> result[Reader, str] {
    let cr = conn_to(cl, node, purpose, deadline);
    guard let c = cr else let e = err_of(cr) { return err(e); }
    let vr = check_api(c, key);
    guard let _v = vr else let e = err_of(vr) { return err(e); }
    return call(c, key, body, deadline);
}

// A request to any broker.
pub fn call_any(cl: Client, key: int, body: Buf, deadline: until) -> result[Reader, str] {
    let cr = any_conn(cl, deadline);
    guard let c = cr else let e = err_of(cr) { return err(e); }
    let vr = check_api(c, key);
    guard let _v = vr else let e = err_of(vr) { return err(e); }
    return call(c, key, body, deadline);
}
