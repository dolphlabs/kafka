// The slang side of bench/run.sh. BENCH_MODE:
//   produce  N records of SIZE bytes to TOPIC (acks=all, idempotent, CODEC)
//   consume  N records from TOPIC's start
//   latency  N records one at a time (linger 0); a consumer measures the
//            time from send to receipt; prints p50/p99 in microseconds
// One line out: "slang MODE records=.. ms=.. rec/s=.. MB/s=.." (+ p50/p99).

import "kafka";
import "proc";
import "sort";
import "strings";
import "time";

fn env_int(name: str, d: int) -> int {
    return to_int(proc.getenv(name) ?? "") ?? d;
}

let brokers = strings.split(proc.getenv("BENCH_BROKERS") ?? "localhost:19092", ",");
let mode = proc.getenv("BENCH_MODE") ?? "produce";
let topic = proc.getenv("BENCH_TOPIC") ?? "bench";
let n = env_int("BENCH_N", 1000000);
let size = env_int("BENCH_SIZE", 100);
let codec = env_int("BENCH_CODEC", 0);
let clr = kafka.connect(kafka.config(brokers), kafka.after_ms(30000));
guard let cl = clr else let e = err_of(clr) { panic(e); }

fn rate(label: str, count: int, nbytes: int, t0: int) {
    let ns = (time.mono() as int) - t0;
    let ms = ns / 1000000;
    println("slang " + label + " records=" + to_str(count) + " ms=" + to_str(ms) + " rec/s=" +
            to_str(count * 1000000000 / ns) + " MB/s=" + to_str(nbytes * 1000 / ns));
}

if mode == "produce" {
    let o = kafka.producer_options();
    o.compression = codec;
    let pr = kafka.producer(cl, o);
    guard let p = pr else let e = err_of(pr) { panic(e); }
    let value = strings.bytes_zero(size);
    for i in 0..size {
        value[i] = 97 + (i % 26);
    }
    let t0 = time.mono() as int;
    for i in 0..n {
        let r = kafka.Record { topic: topic, partition: -1, key: none, value: some(value), headers: [], timestamp: -1 };
        while true {
            let sr = kafka.send(p, r);
            if let _s = sr {
                break;
            }
            // the buffer is full: let the sender catch up
            time.sleep(200000);
        }
    }
    let fr = kafka.flush(p, kafka.after_ms(300000));
    guard let _f = fr else let e = err_of(fr) { panic(e); }
    rate("produce", n, n * size, t0);
    kafka.close_producer(p, kafka.after_ms(10000));
} else if mode == "consume" {
    let info = kafka.describe_topic(cl, topic, kafka.after_ms(10000)) ?? kafka.TopicInfo { name: "", error_code: 0, internal: false, partitions: [] };
    let tps: [kafka.TopicPartition] = [];
    for i in 0..len(info.partitions) {
        push(tps, kafka.tp(topic, i));
    }
    let o = kafka.consumer_options("");
    o.reset = kafka.Reset.Earliest;
    let cr = kafka.assign(cl, tps, o, kafka.after_ms(10000));
    guard let c = cr else let e = err_of(cr) { panic(e); }
    let got = 0;
    let total = 0;
    let t0 = time.mono() as int;
    while got < n {
        let mr = kafka.poll(c, kafka.after_ms(5000));
        guard let ms = mr else let e = err_of(mr) { panic(e); }
        if len(ms) == 0 {
            break;
        }
        got += len(ms);
        for m in ms {
            total += len(m.value ?? b"");
        }
    }
    rate("consume", got, total, t0);
    kafka.close_consumer(c, kafka.after_ms(5000));
} else {
    let o = kafka.consumer_options("");
    o.reset = kafka.Reset.Latest;
    o.fetch_max_wait_ms = 100;
    let cr = kafka.assign(cl, [kafka.tp(topic, 0)], o, kafka.after_ms(10000));
    guard let c = cr else let e = err_of(cr) { panic(e); }
    time.sleep(1000000000);
    let po = kafka.producer_options();
    po.linger_ms = 0;
    let pr = kafka.producer(cl, po);
    guard let p = pr else let e = err_of(pr) { panic(e); }
    let lat: [int] = [];
    for i in 0..n {
        let sent = time.mono() as int;
        let dr = kafka.produce(p, kafka.Record { topic: topic, partition: 0, key: none, value: some(to_bytes(to_str(sent))),
                                                 headers: [], timestamp: -1 }, kafka.after_ms(10000));
        guard let _d = dr else let e = err_of(dr) { panic(e); }
        let got = false;
        while !got {
            let mr = kafka.poll(c, kafka.after_ms(5000));
            guard let ms = mr else let e = err_of(mr) { panic(e); }
            for m in ms {
                let t = to_int(to_str(m.value ?? b"")) ?? 0;
                push(lat, ((time.mono() as int) - t) / 1000);
                got = true;
            }
            if len(ms) == 0 {
                break;
            }
        }
    }
    sort.ints(lat);
    println("slang latency records=" + to_str(len(lat)) + " p50_us=" + to_str(lat[len(lat) / 2]) + " p99_us=" +
            to_str(lat[len(lat) * 99 / 100]));
    kafka.close_producer(p, kafka.after_ms(5000));
    kafka.close_consumer(c, kafka.after_ms(5000));
}
kafka.close(cl);
