// Record batches and message sets written by franz-go (vectors.sl, from
// gen/), decoded, printed, and where the vector says so encoded again
// byte for byte; and varints from Go's encoding/binary both ways. The
// output must match expected.txt, which gen/ writes from its own specs.
// Run with tests/run.sh.

import "crypto";
import "encoding";
import "kafka";

gc struct BatchVec {
    name: str,
    hex: str,
    reencode: bool,
}

gc struct VarVec {
    kind: str,
    value: int,
    hex: str,
}

fn short(b: opt[bytes]) -> str {
    guard let v = b else { return "null"; }
    if len(v) <= 32 {
        return encoding.hex_encode(v);
    }
    return "#" + to_str(len(v)) + ":" + encoding.hex_encode(crypto.sha256(v)[0..8]);
}

fn unhex(s: str) -> bytes {
    let r = encoding.hex_decode(s);
    guard let b = r else { panic("bad hex"); }
    return b;
}

fn run_batch(t: kafka.Crc, v: BatchVec) {
    println("batch " + v.name);
    let data = unhex(v.hex);
    let r = kafka.reader(data, 0, len(data));
    let specs: [kafka.BatchSpec] = [];
    let groups: [[kafka.Record]] = [];
    while true {
        let br = kafka.read_batch(r, t, true);
        guard let got = br else let e = err_of(br) {
            println("  error");
            return;
        }
        guard let b = got else {
            if r.left() > 0 {
                println("  partial");
            }
            break;
        }
        println("  b base=" + to_str(b.base_offset) + " magic=" + to_str(b.magic) + " codec=" + to_str(b.codec()) +
                " txn=" + to_str(b.is_transactional()) + " control=" + to_str(b.is_control()) + " pid=" +
                to_str(b.producer_id) + " epoch=" + to_str(b.producer_epoch) + " seq=" + to_str(b.base_sequence) +
                " count=" + to_str(b.count) + " next=" + to_str(b.next_offset()));
        if b.is_control() {
            let cr = kafka.control_type(t, b);
            guard let ct = cr else let e = err_of(cr) {
                println("  error");
                return;
            }
            println("  control type=" + to_str(ct));
        }
        let mr = kafka.batch_messages(t, b, "t", 0, 1048576);
        guard let ms = mr else let e = err_of(mr) {
            println("  error");
            return;
        }
        let recs: [kafka.Record] = [];
        for m in ms {
            if !b.is_control() {
                let hs = "";
                for h in m.headers {
                    if hs != "" {
                        hs = hs + ",";
                    }
                    hs = hs + h.key + "=" + short(h.value);
                }
                println("  m off=" + to_str(m.offset) + " ts=" + to_str(m.timestamp) + " lat=" +
                        to_str(m.log_append_time) + " key=" + short(m.key) + " value=" + short(m.value) + " h=[" + hs +
                        "]");
            }
            push(recs, kafka.Record { topic: "t", partition: 0, key: m.key, value: m.value, headers: m.headers,
                                      timestamp: m.timestamp });
        }
        let s = kafka.batch_spec();
        s.base_offset = b.base_offset;
        s.leader_epoch = b.leader_epoch;
        s.base_timestamp = b.base_timestamp;
        s.max_timestamp = b.max_timestamp;
        s.producer_id = b.producer_id;
        s.producer_epoch = b.producer_epoch;
        s.base_sequence = b.base_sequence;
        s.transactional = b.is_transactional();
        s.control = b.is_control();
        s.log_append_time = b.log_append_time();
        push(specs, s);
        push(groups, recs);
    }
    if !v.reencode {
        return;
    }
    let out: [bytes] = [];
    for i in 0..len(specs) {
        let er = kafka.encode_batch(t, specs[i], groups[i], 0, len(groups[i]));
        guard let w = er else let e = err_of(er) {
            println("  reencode error: " + e);
            return;
        }
        push(out, w.finish());
    }
    let joined = b"";
    for p in out {
        joined = joined + p;
    }
    if joined == data {
        println("  reencode same");
    } else {
        println("  reencode DIFF " + encoding.hex_encode(joined));
    }
}

fn run_var(v: VarVec) {
    let want = unhex(v.hex);
    let w = kafka.new_buf();
    if v.kind == "varint" {
        kafka.wr_varint(w, v.value);
    } else if v.kind == "varlong" {
        kafka.wr_varlong(w, v.value);
    } else {
        kafka.wr_uvarint(w, v.value);
    }
    let got = w.finish();
    let r = kafka.reader(want, 0, len(want));
    let back = 0;
    if v.kind == "varint" {
        back = kafka.rd_varint(r);
    } else if v.kind == "varlong" {
        back = kafka.rd_varlong(r);
    } else {
        back = kafka.rd_uvarint(r);
    }
    if got == want && back == v.value && r.left() == 0 && r.good() {
        println(v.kind + " " + to_str(v.value) + " ok");
    } else {
        println(v.kind + " " + to_str(v.value) + " mismatch: wrote " + encoding.hex_encode(got) + " read " +
                to_str(back));
    }
}

let t = kafka.crc_tables();
for v in batch_vectors() {
    run_batch(t, v);
}
for v in varint_vectors() {
    run_var(v);
}
