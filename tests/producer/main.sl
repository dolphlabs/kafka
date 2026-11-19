// The producer against kfake (tests/kfake), records read back with raw
// fetches: keyed and sticky partitioning, ordering, headers and nulls,
// every codec, acks 0/1/all, idempotence across injected errors, leader
// moves and dropped connections, the buffer limit, unknown topics and
// oversized records. Run by tests/run.sh (KAFKA_PLAIN, KAFKA_CTL).

import "kafka";
import "net";
import "proc";
import "strings";

fn soon() -> until {
    return kafka.after_ms(15000);
}

fn ctl(cmd: str) {
    let t = kafka.split_host_port(proc.getenv("KAFKA_CTL") ?? "") ?? kafka.Target { host: "", port: 0 };
    let dr = net.dial_until(t.host, t.port, soon());
    guard let fd = dr else let e = err_of(dr) { panic("ctl: " + e); }
    net.send_until(fd as int, to_bytes(cmd + "\n"), soon());
    let rr = net.recv_until(fd as int, 256, soon());
    let reply = to_str(rr ?? b"?");
    net.close(fd as int);
    if !strings.has_prefix(reply, "ok") {
        panic("ctl " + cmd + ": " + reply);
    }
}

// Every record of topic/partition, fetched from offset 0 to the end.
fn read_all(cl: kafka.Client, topic: str, partition: int) -> [kafka.Message] {
    let out: [kafka.Message] = [];
    let end = kafka.list_offset(cl, topic, partition, kafka.OFFSET_LATEST, soon()) ?? 0;
    let offset = 0;
    while offset < end {
        let leader = kafka.leader_of(cl, topic, partition);
        let f = kafka.FetchReq { max_wait_ms: 100, min_bytes: 1, max_bytes: 52428800, isolation: kafka.READ_UNCOMMITTED,
                                 session_id: 0, session_epoch: -1,
                                 topics: [kafka.FetchTopic { topic: topic,
                                                             parts: [kafka.FetchPart { partition: partition, leader_epoch: -1,
                                                                                       offset: offset, last_fetched_epoch: -1,
                                                                                       max_bytes: 10485760 }] }],
                                 forgotten: [], rack: "" };
        let body = kafka.new_buf();
        kafka.enc_fetch(body, f);
        let rr = kafka.call_node(cl, leader, "fetch", kafka.API_FETCH, body, soon());
        guard let r = rr else let e = err_of(rr) { panic(e); }
        let fr = kafka.dec_fetch(r);
        guard let res = fr else let e = err_of(fr) { panic(e); }
        let before = offset;
        for p in res.parts {
            if p.error_code != 0 {
                panic("fetch: " + kafka.error_text(p.error_code));
            }
            let br = kafka.reader(res.frame, p.records_from, p.records_to);
            while true {
                let got = kafka.read_batch(br, cl.crc, true);
                guard let maybe = got else let e = err_of(got) { panic(e); }
                guard let b = maybe else { break; }
                let ms = kafka.batch_messages(cl.crc, b, topic, partition, 52428800) ?? [];
                for m in ms {
                    if m.offset >= offset {
                        push(out, m);
                    }
                }
                offset = b.next_offset();
            }
        }
        if offset == before {
            panic("fetch made no progress at " + to_str(offset));
        }
    }
    return out;
}

fn opts() -> kafka.ProducerOptions {
    return kafka.producer_options();
}

fn keyed_order(cl: kafka.Client) {
    kafka.create_topic(cl, "keyed", 3, 1, soon());
    let pr = kafka.producer(cl, opts());
    guard let p = pr else let e = err_of(pr) { println("producer: " + e); return; }
    println("idempotent producer id assigned: " + to_str(kafka.producer_id(p) >= 0));
    let pend: [kafka.Pending] = [];
    for i in 0..1000 {
        let rec = kafka.record("keyed", to_bytes("k" + to_str(i % 50)), to_bytes("v" + to_str(i)));
        kafka.add_header(rec, "i", to_bytes(to_str(i)));
        let sr = kafka.send(p, rec);
        guard let x = sr else let e = err_of(sr) { panic(e); }
        push(pend, x);
    }
    show("flush", kafka.flush(p, soon()));
    let wrong_part = 0;
    let next: map[int]int = {0: 0, 1: 0, 2: 0};
    let out_of_order = 0;
    for i in 0..len(pend) {
        let dr = kafka.wait(p, pend[i], soon());
        guard let d = dr else let e = err_of(dr) { panic(e); }
        if d.partition != kafka.key_partition(to_bytes("k" + to_str(i % 50)), 3) {
            wrong_part += 1;
        }
        if d.offset != next[d.partition] {
            out_of_order += 1;
        }
        next[d.partition] = d.offset + 1;
    }
    println("keyed: 1000 acknowledged, wrong partition " + to_str(wrong_part) + ", offsets out of order " +
            to_str(out_of_order) + ", total " + to_str(next[0] + next[1] + next[2]));
    let back = 0;
    let mismatched = 0;
    for part in 0..3 {
        let last = -1;
        for m in read_all(cl, "keyed", part) {
            let i = to_int(to_str(kafka.header(m, "i") ?? b"x")) ?? -1;
            if to_str(m.value ?? b"") != "v" + to_str(i) || i <= last {
                mismatched += 1;
            }
            last = i;
            back += 1;
        }
    }
    println("keyed: read back " + to_str(back) + ", out of place " + to_str(mismatched));
    kafka.close_producer(p, soon());
}

fn show(what: str, r: result[bool, str]) {
    if let v = r {
        println(what + ": ok");
    } else let e = err_of(r) {
        println(what + ": " + e);
    }
}

fn sticky_and_nulls(cl: kafka.Client) {
    kafka.create_topic(cl, "sticky", 3, 1, soon());
    let o = opts();
    o.batch_bytes = 2048;
    let pr = kafka.producer(cl, o);
    guard let p = pr else let e = err_of(pr) { println("producer: " + e); return; }
    let counts: map[int]int = {0: 0, 1: 0, 2: 0};
    let pend: [kafka.Pending] = [];
    for i in 0..300 {
        let sr = kafka.send(p, kafka.value_record("sticky", to_bytes("unkeyed value number " + to_str(i))));
        guard let x = sr else let e = err_of(sr) { panic(e); }
        push(pend, x);
    }
    kafka.flush(p, soon());
    for x in pend {
        let d = kafka.wait(p, x, soon()) ?? kafka.Produced { topic: "", partition: -1, offset: -1, timestamp: 0 };
        counts[d.partition] = counts[d.partition] + 1;
    }
    println("sticky: every partition used " + to_str(counts[0] > 0 && counts[1] > 0 && counts[2] > 0) + ", total " +
            to_str(counts[0] + counts[1] + counts[2]));
    let tomb = kafka.Record { topic: "sticky", partition: 1, key: some(b""), value: none,
                              headers: [kafka.Header { key: "h", value: none }], timestamp: 1234 };
    let dr = kafka.produce(p, tomb, soon());
    guard let d = dr else let e = err_of(dr) { panic(e); }
    let ms = read_all(cl, "sticky", 1);
    let last = ms[len(ms) - 1];
    println("tombstone: offset matches " + to_str(last.offset == d.offset) + ", key empty " +
            to_str(len(last.key ?? b"x") == 0) + ", value null " + to_str(!is_some(last.value)) + ", header null " +
            to_str(!is_some(last.headers[0].value)) + ", timestamp " + to_str(last.timestamp));
    kafka.close_producer(p, soon());
}

fn is_some(v: opt[bytes]) -> bool {
    if let _x = v {
        return true;
    }
    return false;
}

fn codecs(cl: kafka.Client) {
    for c in [kafka.CODEC_GZIP, kafka.CODEC_SNAPPY, kafka.CODEC_LZ4, kafka.CODEC_ZSTD] {
        let topic = "codec-" + kafka.codec_name(c);
        kafka.create_topic(cl, topic, 1, 1, soon());
        let o = opts();
        o.compression = c;
        let pr = kafka.producer(cl, o);
        guard let p = pr else let e = err_of(pr) { println("producer: " + e); return; }
        for i in 0..500 {
            kafka.send(p, kafka.record(topic, to_bytes("k"), to_bytes("{\"n\":" + to_str(i) + ",\"pad\":\"aaaaaaaaaaaaaaaa\"}")));
        }
        show(topic + " flush", kafka.flush(p, soon()));
        let ms = read_all(cl, topic, 0);
        let good = len(ms) == 500;
        for i in 0..len(ms) {
            if to_str(ms[i].value ?? b"") != "{\"n\":" + to_str(i) + ",\"pad\":\"aaaaaaaaaaaaaaaa\"}" {
                good = false;
            }
        }
        println(topic + ": 500 read back intact " + to_str(good));
        kafka.close_producer(p, soon());
    }
}

fn acks(cl: kafka.Client) {
    kafka.create_topic(cl, "acks", 1, 1, soon());
    for a in [1, 0] {
        let o = opts();
        o.idempotent = false;
        o.acks = a;
        let pr = kafka.producer(cl, o);
        guard let p = pr else let e = err_of(pr) { println("producer: " + e); return; }
        let dr = kafka.produce(p, kafka.record("acks", b"k", b"v"), soon());
        if let d = dr {
            println("acks=" + to_str(a) + ": offset known " + to_str(d.offset >= 0) + ", producer id " +
                    to_str(kafka.producer_id(p)));
        } else let e = err_of(dr) {
            println("acks=" + to_str(a) + ": " + e);
        }
        kafka.close_producer(p, soon());
    }
    let bad = opts();
    bad.acks = 1;
    println("idempotent acks=1: " + err_of_p(kafka.producer(cl, bad)));
}

fn err_of_p(r: result[kafka.Producer, str]) -> str {
    if let p = r {
        kafka.close_producer(p, soon());
        return "accepted";
    }
    return err_of(r);
}

fn faults(cl: kafka.Client) {
    kafka.create_topic(cl, "faults", 1, 1, soon());
    let pr = kafka.producer(cl, opts());
    guard let p = pr else let e = err_of(pr) { println("producer: " + e); return; }
    kafka.produce(p, kafka.record("faults", b"k", b"warm-up"), soon());
    // retriable errors: retried, delivered once
    ctl("produce-error 3 6");
    let n = 0;
    for i in 0..20 {
        if let _x = kafka.send(p, kafka.record("faults", b"k", to_bytes("a" + to_str(i)))) {
            n += 1;
        }
    }
    show("after 3 NOT_LEADER_OR_FOLLOWER", kafka.flush(p, soon()));
    // dropped connections
    ctl("produce-drop 2");
    for i in 0..20 {
        kafka.send(p, kafka.record("faults", b"k", to_bytes("b" + to_str(i))));
    }
    show("after 2 dropped connections", kafka.flush(p, soon()));
    // a leader move between brokers
    let before = kafka.leader_of(cl, "faults", 0);
    let target = (before + 1) % 3;
    ctl("move faults 0 " + to_str(target));
    for i in 0..20 {
        kafka.send(p, kafka.record("faults", b"k", to_bytes("c" + to_str(i))));
    }
    show("after a leader move", kafka.flush(p, soon()));
    println("leader moved to the target: " + to_str(kafka.leader_of(cl, "faults", 0) == target));
    // a non-retriable error fails the batch
    ctl("produce-error 1 10");
    let dr = kafka.produce(p, kafka.record("faults", b"k", b"too big"), soon());
    println("MESSAGE_TOO_LARGE: " + err_text(dr));
    let ms = read_all(cl, "faults", 0);
    let seen: map[str]int = {};
    let dups = 0;
    for m in ms {
        let v = to_str(m.value ?? b"");
        if has(seen, v) {
            dups += 1;
        }
        seen[v] = 1;
    }
    let ordered = true;
    for prefix in ["a", "b", "c"] {
        let last = -1;
        for m in ms {
            let v = to_str(m.value ?? b"");
            if strings.has_prefix(v, prefix) {
                let k = to_int(strings.slice(v, 1, len(v))) ?? -1;
                if k != last + 1 {
                    ordered = false;
                }
                last = k;
            }
        }
    }
    println("faults: " + to_str(len(ms)) + " records, duplicates " + to_str(dups) + ", in order " + to_str(ordered));
    kafka.close_producer(p, soon());
}

fn err_text(r: result[kafka.Produced, str]) -> str {
    if let d = r {
        return "ok offset " + to_str(d.offset);
    }
    return err_of(r);
}

fn limits(cl: kafka.Client) {
    kafka.create_topic(cl, "limits", 1, 1, soon());
    let o = opts();
    o.batch_bytes = 1024;
    o.buffer_bytes = 2048;
    o.linger_ms = 1000;
    let pr = kafka.producer(cl, o);
    guard let p = pr else let e = err_of(pr) { println("producer: " + e); return; }
    let refused = "";
    for i in 0..100 {
        let sr = kafka.send(p, kafka.record("limits", b"k", strings_bytes(100)));
        if let _x = sr {
        } else let e = err_of(sr) {
            refused = e;
            break;
        }
    }
    println("buffer: " + refused);
    kafka.flush(p, soon());
    println("buffer drained: " + to_str(kafka.buffered_bytes(p)));
    kafka.close_producer(p, soon());
    let o2 = opts();
    o2.max_block_ms = 1000;
    let pr2 = kafka.producer(cl, o2);
    guard let p2 = pr2 else let e = err_of(pr2) { println("producer: " + e); return; }
    println("unknown topic: " + err_text(kafka.produce(p2, kafka.record("no-such-topic", b"k", b"v"), soon())));
    println("no partition 5: " + err_text(kafka.produce(p2, kafka.Record { topic: "limits", partition: 5, key: none,
                                                                         value: some(b"v"), headers: [], timestamp: -1 },
                                                         soon())));
    kafka.close_producer(p2, soon());
    println("send after close: " + err_text(kafka.produce(p2, kafka.record("limits", b"k", b"v"), soon())));
}

fn strings_bytes(n: int) -> bytes {
    return strings.bytes_zero(n);
}

let clr = kafka.connect(kafka.config(strings.split(proc.getenv("KAFKA_PLAIN") ?? "", ",")), soon());
guard let cl = clr else let e = err_of(clr) { panic(e); }
keyed_order(cl);
sticky_and_nulls(cl);
codecs(cl);
acks(cl);
faults(cl);
limits(cl);
kafka.close(cl);
