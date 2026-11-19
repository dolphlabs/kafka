// The consumer against kfake (tests/kfake): assigned partitions with
// seek, pause, resume and every reset policy; an offset deleted from under
// it; a group of one that commits and resumes; a group of two that splits
// the partitions, shares the records without loss or duplicates, and
// rebalances when one leaves; static membership. Run by tests/run.sh.

import "kafka";
import "proc";
import "strings";
import "time";

fn soon() -> until {
    return kafka.after_ms(20000);
}

fn produce_n(cl: kafka.Client, topic: str, n: int, from: int) {
    let pr = kafka.producer(cl, kafka.producer_options());
    guard let p = pr else let e = err_of(pr) { panic(e); }
    for i in from..(from + n) {
        let sr = kafka.send(p, kafka.record(topic, to_bytes("k" + to_str(i % 17)), to_bytes(to_str(i))));
        guard let _s = sr else let e = err_of(sr) { panic(e); }
    }
    let fr = kafka.close_producer(p, soon());
    guard let _f = fr else let e = err_of(fr) { panic(e); }
}

// Polls until n records came or the deadline passed.
fn take_n(c: kafka.Consumer, n: int, ms: int) -> [kafka.Message] {
    let out: [kafka.Message] = [];
    let dl = kafka.after_ms(ms);
    while len(out) < n && !until_hit(dl) {
        let pr = kafka.poll(c, kafka.after_ms(200));
        guard let got = pr else let e = err_of(pr) {
            println("poll: " + e);
            return out;
        }
        for m in got {
            push(out, m);
        }
    }
    return out;
}

fn in_order(ms: [kafka.Message]) -> bool {
    let last: map[int]int = {};
    for m in ms {
        if has(last, m.partition) && m.offset != last[m.partition] + 1 {
            return false;
        }
        last[m.partition] = m.offset;
    }
    return true;
}

fn assigned(cl: kafka.Client) {
    kafka.create_topic(cl, "c-assign", 3, 1, soon());
    produce_n(cl, "c-assign", 300, 0);
    let o = kafka.consumer_options("");
    o.reset = kafka.Reset.Earliest;
    o.max_poll_records = 50;
    let cr = kafka.assign(cl, [kafka.tp("c-assign", 0), kafka.tp("c-assign", 1), kafka.tp("c-assign", 2)], o, soon());
    guard let c = cr else let e = err_of(cr) { println("assign: " + e); return; }
    let ms = take_n(c, 300, 10000);
    println("assign: " + to_str(len(ms)) + " records, in order " + to_str(in_order(ms)) + ", at most 50 a poll");
    kafka.seek(c, "c-assign", 0, 10);
    kafka.pause(c, "c-assign", 1);
    produce_n(cl, "c-assign", 30, 300);
    let after = take_n(c, 1000, 1500);
    let first_p0 = -1;
    let p1 = 0;
    for m in after {
        if m.partition == 0 && first_p0 < 0 {
            first_p0 = m.offset;
        }
        if m.partition == 1 {
            p1 += 1;
        }
    }
    println("seek p0 to 10: first offset " + to_str(first_p0) + "; paused p1 records " + to_str(p1));
    kafka.resume(c, "c-assign", 1);
    let resumed = take_n(c, 1000, 1500);
    let p1r = 0;
    for m in resumed {
        if m.partition == 1 {
            p1r += 1;
        }
    }
    println("resumed p1 delivered records: " + to_str(p1r > 0));
    // records gone from under the position: reset to the earliest left
    kafka.delete_records(cl, "c-assign", 2, 50, soon());
    kafka.seek(c, "c-assign", 2, 5);
    let reset = take_n(c, 1, 3000);
    if len(reset) > 0 {
        println("out of range at 5 after deleting below 50: restarted at " + to_str(reset[0].offset));
    }
    kafka.close_consumer(c, soon());
    println("poll after close: " + err_of_or(kafka.poll(c, kafka.after_ms(10))));
}

fn err_of_or(r: result[[kafka.Message], str]) -> str {
    if let ms = r {
        return "ok " + to_str(len(ms));
    }
    return err_of(r);
}

fn resets(cl: kafka.Client) {
    kafka.create_topic(cl, "c-reset", 1, 1, soon());
    produce_n(cl, "c-reset", 20, 0);
    let o = kafka.consumer_options("");
    o.reset = kafka.Reset.Latest;
    let cr = kafka.assign(cl, [kafka.tp("c-reset", 0)], o, soon());
    guard let c = cr else let e = err_of(cr) { println("assign: " + e); return; }
    let none_yet = take_n(c, 1, 1000);
    produce_n(cl, "c-reset", 5, 20);
    let fresh = take_n(c, 5, 5000);
    println("reset latest: " + to_str(len(none_yet)) + " old, then " + to_str(len(fresh)) + " new from offset " +
            to_str(fresh[0].offset));
    kafka.close_consumer(c, soon());
    let on = kafka.consumer_options("never-committed");
    on.reset = kafka.Reset.None;
    let cr2 = kafka.assign(cl, [kafka.tp("c-reset", 0)], on, soon());
    guard let c2 = cr2 else let e = err_of(cr2) { println("assign: " + e); return; }
    println("reset none: " + err_of_or(kafka.poll(c2, kafka.after_ms(3000))));
    kafka.close_consumer(c2, soon());
}

fn group_of_one(cl: kafka.Client) {
    kafka.create_topic(cl, "c-group1", 2, 1, soon());
    produce_n(cl, "c-group1", 200, 0);
    let o = kafka.consumer_options("g-one");
    o.reset = kafka.Reset.Earliest;
    o.auto_commit = false;
    let cr = kafka.subscribe(cl, ["c-group1"], o, soon());
    guard let c = cr else let e = err_of(cr) { println("subscribe: " + e); return; }
    let ms = take_n(c, 200, 15000);
    println("group of one: " + to_str(len(ms)) + " records, in order " + to_str(in_order(ms)) + ", partitions " +
            to_str(len(kafka.assignment(c))) + ", generation set " + to_str(kafka.generation(c) >= 1));
    show("commit", kafka.commit(c, soon()));
    show("close", kafka.close_consumer(c, soon()));
    let sum = 0;
    for x in kafka.committed_offsets(cl, "g-one", soon()) ?? [] {
        sum += x.offset;
    }
    println("committed offsets sum to " + to_str(sum));
    produce_n(cl, "c-group1", 10, 200);
    let cr2 = kafka.subscribe(cl, ["c-group1"], o, soon());
    guard let c2 = cr2 else let e = err_of(cr2) { println("subscribe: " + e); return; }
    let again = take_n(c2, 1000, 6000);
    let lowest = 1000000;
    for m in again {
        let v = to_int(to_str(m.value ?? b"")) ?? -1;
        if v < lowest {
            lowest = v;
        }
    }
    println("resumed from the commit: " + to_str(len(again)) + " records, lowest value " + to_str(lowest));
    kafka.close_consumer(c2, soon());
}

fn show(what: str, r: result[bool, str]) {
    if let v = r {
        println(what + ": ok");
    } else let e = err_of(r) {
        println(what + ": " + e);
    }
}

gc struct Sink {
    values: map[str]int,
    dups: int,
    stop: bool,
}

fn drain(c: kafka.Consumer, s: Sink, lock: mutex) -> int {
    while !s.stop {
        let pr = kafka.poll(c, kafka.after_ms(100));
        guard let ms = pr else { break; }
        mutex_lock(lock);
        for m in ms {
            let v = to_str(m.value ?? b"");
            if has(s.values, v) {
                s.dups += 1;
            }
            s.values[v] = 1;
        }
        mutex_unlock(lock);
    }
    return 0;
}

fn wait_split(a: kafka.Consumer, b: kafka.Consumer, total: int) -> bool {
    let dl = kafka.after_ms(20000);
    while !until_hit(dl) {
        let x = len(kafka.assignment(a));
        let y = len(kafka.assignment(b));
        if x > 0 && y > 0 && x + y == total && kafka.generation(a) == kafka.generation(b) {
            return true;
        }
        time.sleep(50000000);
    }
    return false;
}

fn group_of_two(cl: kafka.Client) {
    kafka.create_topic(cl, "c-group2", 6, 1, soon());
    let o = kafka.consumer_options("g-two");
    o.reset = kafka.Reset.Earliest;
    o.assignors = ["roundrobin"];
    o.heartbeat_interval_ms = 300;
    o.auto_commit_interval_ms = 200;
    let ar = kafka.subscribe(cl, ["c-group2"], o, soon());
    guard let a = ar else let e = err_of(ar) { println("subscribe a: " + e); return; }
    let br = kafka.subscribe(cl, ["c-group2"], o, soon());
    guard let b = br else let e = err_of(br) { println("subscribe b: " + e); return; }
    println("two members split six partitions: " + to_str(wait_split(a, b, 6)) + " (" +
            to_str(len(kafka.assignment(a))) + "+" + to_str(len(kafka.assignment(b))) + ")");
    let gd = kafka.describe_group(cl, "g-two", soon());
    if let g = gd {
        println("describe g-two: " + g.state + ", " + to_str(len(g.members)) + " members, protocol " + g.protocol);
    }
    let s = Sink { values: {}, dups: 0, stop: false };
    let lock = make_mutex();
    let ha = spawn drain(a, s, lock);
    let hb = spawn drain(b, s, lock);
    produce_n(cl, "c-group2", 600, 0);
    let dl = kafka.after_ms(15000);
    while len(s.values) < 600 && !until_hit(dl) {
        time.sleep(50000000);
    }
    println("shared: " + to_str(len(s.values)) + " distinct of 600, duplicates " + to_str(s.dups));
    // b leaves: a takes over every partition and the rest of the records
    s.stop = true;
    join_wait(ha);
    join_wait(hb);
    kafka.commit(a, soon());
    kafka.commit(b, soon());
    kafka.close_consumer(b, soon());
    let dl2 = kafka.after_ms(20000);
    while len(kafka.assignment(a)) != 6 && !until_hit(dl2) {
        kafka.poll(a, kafka.after_ms(100));
    }
    println("after b left, a has " + to_str(len(kafka.assignment(a))) + " partitions");
    produce_n(cl, "c-group2", 60, 600);
    let rest = take_n(a, 60, 10000);
    let new_ones = 0;
    for m in rest {
        if (to_int(to_str(m.value ?? b"")) ?? 0) >= 600 {
            new_ones += 1;
        }
    }
    println("a then read " + to_str(new_ones) + " of the 60 new records");
    kafka.close_consumer(a, soon());
}

fn static_member(cl: kafka.Client) {
    kafka.create_topic(cl, "c-static", 4, 1, soon());
    let o = kafka.consumer_options("g-static");
    o.instance_id = "worker-1";
    o.reset = kafka.Reset.Earliest;
    let cr = kafka.subscribe(cl, ["c-static"], o, soon());
    guard let c = cr else let e = err_of(cr) { println("subscribe: " + e); return; }
    let dl = kafka.after_ms(15000);
    while len(kafka.assignment(c)) == 0 && !until_hit(dl) {
        kafka.poll(c, kafka.after_ms(100));
    }
    let first = kafka.member_id(c);
    kafka.close_consumer(c, soon());
    let cr2 = kafka.subscribe(cl, ["c-static"], o, soon());
    guard let c2 = cr2 else let e = err_of(cr2) { println("subscribe: " + e); return; }
    let dl2 = kafka.after_ms(15000);
    while len(kafka.assignment(c2)) == 0 && !until_hit(dl2) {
        kafka.poll(c2, kafka.after_ms(100));
    }
    println("static member rejoined with all " + to_str(len(kafka.assignment(c2))) + " partitions, member ids differ " +
            to_str(first != kafka.member_id(c2)));
    kafka.close_consumer(c2, soon());
}

let clr = kafka.connect(kafka.config(strings.split(proc.getenv("KAFKA_PLAIN") ?? "", ",")), soon());
guard let cl = clr else let e = err_of(clr) { panic(e); }
assigned(cl);
resets(cl);
group_of_one(cl);
group_of_two(cl);
static_member(cl);
kafka.close(cl);
