// The client against a real broker in Docker (tests/live/run.sh starts
// Apache Kafka 4.1, Apache Kafka 3.9 and Redpanda in turn): 2000 keyed
// records in order, every codec, a 900 KB record and a 2 MB one refused,
// a group of two, transactions under read_committed, records both ways
// with franz-go (tests/live/peer), SCRAM, and the broker restarted while
// the producer is mid-stream (every record exactly once).
//
// LIVE_BROKERS, LIVE_SASL (empty: skip SASL), LIVE_CONTAINER.

import "kafka";
import "proc";
import "strings";
import "time";

fn soon() -> until {
    return kafka.after_ms(60000);
}

fn show(what: str, r: result[bool, str]) {
    if let _v = r {
        println(what + ": ok");
    } else let e = err_of(r) {
        println(what + ": " + e);
    }
}

fn read_all(cl: kafka.Client, tps: [kafka.TopicPartition], n: int, isolation: int) -> [kafka.Message] {
    let o = kafka.consumer_options("");
    o.reset = kafka.Reset.Earliest;
    o.isolation = isolation;
    let cr = kafka.assign(cl, tps, o, soon());
    guard let c = cr else let e = err_of(cr) { panic(e); }
    let out: [kafka.Message] = [];
    let quiet = 0;
    let dl = kafka.after_ms(60000);
    while quiet < 10 && !until_hit(dl) {
        let pr = kafka.poll(c, kafka.after_ms(300));
        guard let ms = pr else let e = err_of(pr) { println("poll: " + e); break; }
        if len(ms) == 0 {
            if len(out) >= n {
                quiet += 1;
            }
            continue;
        }
        quiet = 0;
        for m in ms {
            push(out, m);
        }
    }
    kafka.close_consumer(c, soon());
    return out;
}

fn parts(topic: str, n: int) -> [kafka.TopicPartition] {
    let out: [kafka.TopicPartition] = [];
    for i in 0..n {
        push(out, kafka.tp(topic, i));
    }
    return out;
}

fn in_order(ms: [kafka.Message]) -> bool {
    let last: map[int]int = {};
    for m in ms {
        if has(last, m.partition) && m.offset <= last[m.partition] {
            return false;
        }
        last[m.partition] = m.offset;
    }
    return true;
}

fn placed(ms: [kafka.Message], n: int) -> bool {
    for m in ms {
        if m.partition != kafka.key_partition(m.key ?? b"", n) {
            return false;
        }
    }
    return true;
}

fn keyed(cl: kafka.Client) {
    kafka.create_topic(cl, "live-keyed", 6, 1, soon());
    let o = kafka.producer_options();
    o.compression = kafka.CODEC_LZ4;
    let pr = kafka.producer(cl, o);
    guard let p = pr else let e = err_of(pr) { panic(e); }
    for i in 0..2000 {
        kafka.send(p, kafka.record("live-keyed", to_bytes("key-" + to_str(i % 101)), to_bytes("v" + to_str(i))));
    }
    show("2000 keyed records (lz4)", kafka.close_producer(p, soon()));
    let ms = read_all(cl, parts("live-keyed", 6), 2000, kafka.READ_UNCOMMITTED);
    println("read back " + to_str(len(ms)) + ", in order " + to_str(in_order(ms)) + ", murmur2 placement " +
            to_str(placed(ms, 6)));
}

fn codecs(cl: kafka.Client) {
    for c in [kafka.CODEC_GZIP, kafka.CODEC_SNAPPY, kafka.CODEC_LZ4, kafka.CODEC_ZSTD] {
        let topic = "live-" + kafka.codec_name(c);
        kafka.create_topic(cl, topic, 1, 1, soon());
        let o = kafka.producer_options();
        o.compression = c;
        let pr = kafka.producer(cl, o);
        guard let p = pr else let e = err_of(pr) { panic(e); }
        for i in 0..300 {
            kafka.send(p, kafka.value_record(topic, to_bytes("{\"seq\":" + to_str(i) + ",\"text\":\"compress me\"}")));
        }
        kafka.close_producer(p, soon());
        let ms = read_all(cl, parts(topic, 1), 300, kafka.READ_UNCOMMITTED);
        let good = len(ms) == 300;
        for i in 0..len(ms) {
            if to_str(ms[i].value ?? b"") != "{\"seq\":" + to_str(i) + ",\"text\":\"compress me\"}" {
                good = false;
            }
        }
        println(kafka.codec_name(c) + ": 300 through the broker intact " + to_str(good));
    }
}

fn big(cl: kafka.Client) {
    kafka.create_topic(cl, "live-big", 1, 1, soon());
    let pr = kafka.producer(cl, kafka.producer_options());
    guard let p = pr else let e = err_of(pr) { panic(e); }
    let v = strings.bytes_zero(900000);
    v[899999] = 7;
    let dr = kafka.produce(p, kafka.value_record("live-big", v), soon());
    println("900 KB record: " + to_str(is_ok(dr)));
    let tr = kafka.produce(p, kafka.value_record("live-big", strings.bytes_zero(2000000)), soon());
    let refused = "";
    if let _d = tr {
        refused = "accepted";
    } else let e = err_of(tr) {
        refused = to_str(strings.contains(e, "TOO_LARGE"));
    }
    println("2 MB record refused as too large: " + refused);
    kafka.close_producer(p, soon());
    let ms = read_all(cl, parts("live-big", 1), 1, kafka.READ_UNCOMMITTED);
    println("900 KB record read back intact: " + to_str(len(ms) == 1 && (ms[0].value ?? b"") == v));
}

fn is_ok(r: result[kafka.Produced, str]) -> bool {
    if let _v = r {
        return true;
    }
    println("  " + err_of(r));
    return false;
}

gc struct Sink {
    seen: map[str]int,
    dups: int,
    stop: bool,
    lock: mutex,
}

fn drain(c: kafka.Consumer, s: Sink) -> int {
    while !s.stop {
        let pr = kafka.poll(c, kafka.after_ms(100));
        guard let ms = pr else { break; }
        mutex_lock(s.lock);
        for m in ms {
            let v = to_str(m.value ?? b"");
            if has(s.seen, v) {
                s.dups += 1;
            }
            s.seen[v] = 1;
        }
        mutex_unlock(s.lock);
    }
    return 0;
}

fn group(cl: kafka.Client) {
    kafka.create_topic(cl, "live-group", 4, 1, soon());
    let o = kafka.consumer_options("live-g");
    o.reset = kafka.Reset.Earliest;
    o.heartbeat_interval_ms = 500;
    let ar = kafka.subscribe(cl, ["live-group"], o, soon());
    guard let a = ar else let e = err_of(ar) { println("subscribe: " + e); return; }
    let br = kafka.subscribe(cl, ["live-group"], o, soon());
    guard let b = br else let e = err_of(br) { println("subscribe: " + e); return; }
    let s = Sink { seen: {}, dups: 0, stop: false, lock: make_mutex() };
    let ha = spawn drain(a, s);
    let hb = spawn drain(b, s);
    let dl = kafka.after_ms(60000);
    while (len(kafka.assignment(a)) == 0 || len(kafka.assignment(b)) == 0) && !until_hit(dl) {
        time.sleep(100000000);
    }
    println("group of two: both assigned " + to_str(len(kafka.assignment(a)) + len(kafka.assignment(b)) == 4));
    let pr = kafka.producer(cl, kafka.producer_options());
    guard let p = pr else let e = err_of(pr) { panic(e); }
    for i in 0..600 {
        kafka.send(p, kafka.record("live-group", to_bytes("k" + to_str(i)), to_bytes("g" + to_str(i))));
    }
    kafka.close_producer(p, soon());
    while len(s.seen) < 600 && !until_hit(dl) {
        time.sleep(100000000);
    }
    s.stop = true;
    join_wait(ha);
    join_wait(hb);
    println("group of two shared " + to_str(len(s.seen)) + " of 600, duplicates " + to_str(s.dups));
    kafka.close_consumer(a, soon());
    kafka.close_consumer(b, soon());
}

fn txn(cl: kafka.Client) {
    kafka.create_topic(cl, "live-txn", 1, 1, soon());
    let o = kafka.producer_options();
    o.transactional_id = "live-tx";
    let pr = kafka.producer(cl, o);
    guard let p = pr else let e = err_of(pr) { println("txn producer: " + e); return; }
    for round in 0..3 {
        kafka.begin_transaction(p);
        for i in 0..10 {
            kafka.send(p, kafka.Record { topic: "live-txn", partition: 0, key: none,
                                         value: some(to_bytes("r" + to_str(round) + "-" + to_str(i))), headers: [],
                                         timestamp: -1 });
        }
        if round == 1 {
            show("abort round 1", kafka.abort_transaction(p, soon()));
        } else {
            show("commit round " + to_str(round), kafka.commit_transaction(p, soon()));
        }
    }
    kafka.close_producer(p, soon());
    let rc = read_all(cl, parts("live-txn", 1), 20, kafka.READ_COMMITTED);
    let aborted_seen = false;
    for m in rc {
        if strings.has_prefix(to_str(m.value ?? b""), "r1-") {
            aborted_seen = true;
        }
    }
    println("read_committed: " + to_str(len(rc)) + " records, aborted round visible " + to_str(aborted_seen));
    let ru = read_all(cl, parts("live-txn", 1), 30, kafka.READ_UNCOMMITTED);
    println("read_uncommitted: " + to_str(len(ru)) + " records");
}

fn peer(cl: kafka.Client) {
    let ms = read_all(cl, parts("live-peer", 3), 500, kafka.READ_UNCOMMITTED);
    let headers = true;
    for m in ms {
        if to_str(kafka.header(m, "from") ?? b"") != "franz-go" {
            headers = false;
        }
    }
    println("from franz-go: " + to_str(len(ms)) + " records (zstd), headers " + to_str(headers) +
            ", placement matches key_partition " + to_str(placed(ms, 3)) + ", in order " + to_str(in_order(ms)));
    kafka.create_topic(cl, "live-out", 3, 1, soon());
    let pr = kafka.producer(cl, kafka.producer_options());
    guard let p = pr else let e = err_of(pr) { panic(e); }
    for i in 0..500 {
        let r = kafka.record("live-out", to_bytes("key" + to_str(i % 23)), to_bytes("slang-" + to_str(i)));
        kafka.add_header(r, "from", to_bytes("slang"));
        kafka.send(p, r);
    }
    show("500 records for franz-go", kafka.close_producer(p, soon()));
}

fn sasl() {
    let addr = proc.getenv("LIVE_SASL") ?? "";
    if addr == "" {
        return;
    }
    let cfg = kafka.config([addr]);
    cfg.sasl = kafka.scram_sha512("live", "live-secret");
    let clr = kafka.connect(cfg, soon());
    if let cl = clr {
        let pr = kafka.producer(cl, kafka.producer_options());
        if let p = pr {
            let dr = kafka.produce(p, kafka.record("live-keyed", b"sasl", b"over scram"), soon());
            println("scram-sha-512: produced " + to_str(is_ok(dr)));
            kafka.close_producer(p, soon());
        } else let e = err_of(pr) {
            println("scram producer: " + e);
        }
        kafka.close(cl);
    } else let e = err_of(clr) {
        println("scram-sha-512: " + e);
    }
    let bad = kafka.config([addr]);
    bad.sasl = kafka.scram_sha256("live", "wrong");
    let br = kafka.connect(bad, soon());
    if let cl = br {
        println("wrong password: connected (wrong)");
        kafka.close(cl);
    } else let e = err_of(br) {
        println("wrong password refused: " + to_str(strings.contains(e, "authentication failed")));
    }
}

fn restart(cl: kafka.Client) {
    let container = proc.getenv("LIVE_CONTAINER") ?? "";
    kafka.create_topic(cl, "live-restart", 3, 1, soon());
    let pr = kafka.producer(cl, kafka.producer_options());
    guard let p = pr else let e = err_of(pr) { panic(e); }
    let empty: [str] = [];
    for i in 0..3000 {
        if i == 1500 {
            kafka.flush(p, soon());
            let sr = proc.spawn(["sh", "-c", "docker restart " + container + " > /dev/null"], empty, "", 0);
            if let pid = sr {
                proc.wait(pid, kafka.after_ms(120000));
            }
        }
        let sr = kafka.send(p, kafka.record("live-restart", to_bytes("k" + to_str(i % 7)), to_bytes(to_str(i))));
        guard let _s = sr else let e = err_of(sr) { println("send during restart: " + e); break; }
    }
    show("3000 records across a broker restart", kafka.close_producer(p, kafka.after_ms(120000)));
    let ms = read_all(cl, parts("live-restart", 3), 3000, kafka.READ_UNCOMMITTED);
    let seen: map[str]int = {};
    for m in ms {
        seen[to_str(m.value ?? b"")] = 1;
    }
    println("after the restart: " + to_str(len(ms)) + " records, " + to_str(len(seen)) + " distinct, in order " +
            to_str(in_order(ms)));
}

let clr = kafka.connect(kafka.config(strings.split(proc.getenv("LIVE_BROKERS") ?? "", ",")), soon());
guard let cl = clr else let e = err_of(clr) { panic(e); }
println("connected: " + to_str(len(kafka.brokers(cl))) + " broker, cluster id set " + to_str(kafka.cluster_id(cl) != ""));
keyed(cl);
codecs(cl);
big(cl);
group(cl);
txn(cl);
peer(cl);
sasl();
restart(cl);
kafka.close(cl);
