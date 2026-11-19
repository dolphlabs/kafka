// Transactions against kfake (tests/kfake): commits and aborts as
// read_committed and read_uncommitted consumers see them, an open
// transaction holding read_committed back, a second instance fencing the
// first, exactly-once consume-transform-produce, and misuse. Run by
// tests/run.sh.

import "kafka";
import "proc";
import "strings";

fn soon() -> until {
    return kafka.after_ms(20000);
}

fn txn_producer(cl: kafka.Client, id: str) -> kafka.Producer {
    let o = kafka.producer_options();
    o.transactional_id = id;
    let pr = kafka.producer(cl, o);
    guard let p = pr else let e = err_of(pr) { panic("producer " + id + ": " + e); }
    return p;
}

fn send_n(p: kafka.Producer, topic: str, prefix: str, n: int) {
    for i in 0..n {
        let sr = kafka.send(p, kafka.Record { topic: topic, partition: 0, key: none,
                                              value: some(to_bytes(prefix + to_str(i))), headers: [], timestamp: -1 });
        guard let _s = sr else let e = err_of(sr) { println("send: " + e); return; }
    }
}

fn show(what: str, r: result[bool, str]) {
    if let v = r {
        println(what + ": ok");
    } else let e = err_of(r) {
        println(what + ": " + e);
    }
}

// Every record of topic/0 the isolation level shows, read until a quiet
// second.
fn read_values(cl: kafka.Client, topic: str, isolation: int) -> [str] {
    let o = kafka.consumer_options("");
    o.reset = kafka.Reset.Earliest;
    o.isolation = isolation;
    let cr = kafka.assign(cl, [kafka.tp(topic, 0)], o, soon());
    guard let c = cr else let e = err_of(cr) { panic(e); }
    let out: [str] = [];
    let quiet = 0;
    while quiet < 5 {
        let pr = kafka.poll(c, kafka.after_ms(200));
        guard let ms = pr else let e = err_of(pr) { println("poll: " + e); break; }
        if len(ms) == 0 {
            quiet += 1;
        }
        for m in ms {
            push(out, to_str(m.value ?? b""));
        }
    }
    kafka.close_consumer(c, soon());
    return out;
}

fn summary(vs: [str]) -> str {
    let counts: map[str]int = {};
    let order: [str] = [];
    for v in vs {
        let k = strings.slice(v, 0, 1);
        if !has(counts, k) {
            counts[k] = 0;
            push(order, k);
        }
        counts[k] = counts[k] + 1;
    }
    let parts: [str] = [];
    for k in order {
        push(parts, k + "x" + to_str(counts[k]));
    }
    return strings.join(parts, " ");
}

fn commit_and_abort(cl: kafka.Client) {
    kafka.create_topic(cl, "t-txn", 1, 1, soon());
    let p = txn_producer(cl, "tx-1");
    show("begin", kafka.begin_transaction(p));
    send_n(p, "t-txn", "a", 10);
    show("commit 10", kafka.commit_transaction(p, soon()));
    kafka.begin_transaction(p);
    send_n(p, "t-txn", "b", 5);
    show("abort 5", kafka.abort_transaction(p, soon()));
    kafka.begin_transaction(p);
    send_n(p, "t-txn", "c", 3);
    show("commit 3", kafka.commit_transaction(p, soon()));
    println("read_committed sees: " + summary(read_values(cl, "t-txn", kafka.READ_COMMITTED)));
    println("read_uncommitted sees: " + summary(read_values(cl, "t-txn", kafka.READ_UNCOMMITTED)));
    // an open transaction holds read_committed consumers back
    kafka.begin_transaction(p);
    send_n(p, "t-txn", "d", 4);
    kafka.flush(p, soon());
    println("while open, read_committed sees: " + summary(read_values(cl, "t-txn", kafka.READ_COMMITTED)));
    kafka.commit_transaction(p, soon());
    println("after commit, read_committed sees: " + summary(read_values(cl, "t-txn", kafka.READ_COMMITTED)));
    kafka.close_producer(p, soon());
}

fn fencing(cl: kafka.Client) {
    kafka.create_topic(cl, "t-fence", 1, 1, soon());
    let a = txn_producer(cl, "tx-fence");
    kafka.begin_transaction(a);
    send_n(a, "t-fence", "a", 2);
    show("first instance commits", kafka.commit_transaction(a, soon()));
    let b = txn_producer(cl, "tx-fence");
    println("second instance has a later epoch: " + to_str(kafka.producer_epoch(b) > kafka.producer_epoch(a)));
    kafka.begin_transaction(a);
    send_n(a, "t-fence", "z", 2);
    let r = kafka.commit_transaction(a, soon());
    if let _v = r {
        println("fenced instance commit: ok (wrong)");
    } else let e = err_of(r) {
        println("fenced instance commit refused: " + e);
    }
    kafka.begin_transaction(b);
    send_n(b, "t-fence", "b", 2);
    show("second instance commits", kafka.commit_transaction(b, soon()));
    println("read_committed sees: " + summary(read_values(cl, "t-fence", kafka.READ_COMMITTED)));
    kafka.close_producer(a, kafka.after_ms(2000));
    kafka.close_producer(b, soon());
}

fn exactly_once(cl: kafka.Client) {
    kafka.create_topic(cl, "eos-in", 1, 1, soon());
    kafka.create_topic(cl, "eos-out", 1, 1, soon());
    let pp = kafka.producer(cl, kafka.producer_options()) ?? txn_producer(cl, "unused");
    for i in 0..20 {
        kafka.send(pp, kafka.value_record("eos-in", to_bytes("in" + to_str(i))));
    }
    kafka.close_producer(pp, soon());
    let o = kafka.consumer_options("g-eos");
    o.reset = kafka.Reset.Earliest;
    o.auto_commit = false;
    o.isolation = kafka.READ_COMMITTED;
    let cr = kafka.subscribe(cl, ["eos-in"], o, soon());
    guard let c = cr else let e = err_of(cr) { println("subscribe: " + e); return; }
    let p = txn_producer(cl, "tx-eos");
    let done = 0;
    let dl = kafka.after_ms(20000);
    while done < 20 && !until_hit(dl) {
        let mr = kafka.poll(c, kafka.after_ms(200));
        guard let ms = mr else let e = err_of(mr) { println("poll: " + e); break; }
        if len(ms) == 0 {
            continue;
        }
        kafka.begin_transaction(p);
        for m in ms {
            kafka.send(p, kafka.Record { topic: "eos-out", partition: 0, key: none,
                                         value: some(to_bytes("out-" + to_str(m.value ?? b""))), headers: [],
                                         timestamp: -1 });
        }
        let sr = kafka.send_offsets_to_transaction(p, c, soon());
        guard let _s = sr else let e = err_of(sr) { println("send offsets: " + e); break; }
        let tr = kafka.commit_transaction(p, soon());
        guard let _t = tr else let e = err_of(tr) { println("commit: " + e); break; }
        done += len(ms);
    }
    let sum = 0;
    for x in kafka.committed_offsets(cl, "g-eos", soon()) ?? [] {
        sum += x.offset;
    }
    println("consume-transform-produce: " + to_str(done) + " in, group offset committed in the transaction " +
            to_str(sum));
    println("eos-out read_committed: " + summary(read_values(cl, "eos-out", kafka.READ_COMMITTED)));
    kafka.close_consumer(c, soon());
    kafka.close_producer(p, soon());
}

fn misuse(cl: kafka.Client) {
    let p = txn_producer(cl, "tx-misuse");
    println("send outside a transaction: " + err_of_s(kafka.send(p, kafka.value_record("t-txn", b"x"))));
    show("commit without begin", kafka.commit_transaction(p, soon()));
    kafka.begin_transaction(p);
    show("begin twice", kafka.begin_transaction(p));
    show("empty transaction commits", kafka.commit_transaction(p, soon()));
    kafka.close_producer(p, soon());
    let plain = kafka.producer(cl, kafka.producer_options()) ?? p;
    show("begin without a transactional_id", kafka.begin_transaction(plain));
    kafka.close_producer(plain, soon());
}

fn err_of_s(r: result[kafka.Pending, str]) -> str {
    if let _x = r {
        return "accepted";
    }
    return err_of(r);
}

let clr = kafka.connect(kafka.config(strings.split(proc.getenv("KAFKA_PLAIN") ?? "", ",")), soon());
guard let cl = clr else let e = err_of(clr) { panic(e); }
commit_and_abort(cl);
fencing(cl);
exactly_once(cl);
misuse(cl);
kafka.close(cl);
