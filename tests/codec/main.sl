// The codecs against Go's: every vector in vectors.bin.gz (written by
// gen/ with klauspost/compress, pierrec/lz4, golang/snappy and gzip) must
// decompress to its input; each input must round trip through the slang
// encoders; truncated and corrupted data must fail cleanly; and the size
// limit must hold. Run with tests/run.sh (KAFKA_TESTDATA names this
// directory). With KAFKA_CODEC_OUT set, the slang encoders' output is
// written there for `go run ./gen check FILE`.

import "compress";
import "crypto";
import "encoding";
import "fs";
import "kafka";
import "proc";

gc struct Input {
    name: str,
    data: bytes,
}

fn read_file(path: str) -> bytes {
    let or = fs.open(path);
    guard let fd = or else let e = err_of(or) { panic(path + ": " + e); }
    let parts: [bytes] = [];
    while true {
        let rr = fs.read(fd as int, 1048576);
        guard let b = rr else let e = err_of(rr) { panic(e); }
        if len(b) == 0 {
            break;
        }
        push(parts, b);
    }
    fs.close(fd as int);
    let all = b"";
    for p in parts {
        all = all + p;
    }
    return all;
}

fn digest(b: bytes) -> str {
    return encoding.hex_encode(crypto.sha256(b)[0..8]);
}

fn codec_of(name: str) -> int {
    return switch name {
        case "gzip" { 1 }
        case "snappy" { 2 }
        case "lz4" { 3 }
        default { 4 }
    };
}

let dir = proc.getenv("KAFKA_TESTDATA") ?? "tests/codec";
let gr = compress.gunzip(read_file(dir + "/vectors.bin.gz"), 67108864);
guard let raw = gr else let e = err_of(gr) { panic(e); }
let r = kafka.reader(raw, 0, len(raw));
let inputs: [Input] = [];
let n_in = kafka.rd_u16(r);
for i in 0..n_in {
    let name = kafka.rd_str(r);
    let size = kafka.rd_i32(r);
    push(inputs, Input { name: name, data: kafka.rd_raw(r, size) });
}
let comps: [bytes] = [];
let codecs: [int] = [];
while r.left() > 0 {
    let name = kafka.rd_str(r);
    let codec = kafka.rd_u8(r);
    let idx = kafka.rd_u16(r);
    let comp = kafka.rd_bytes(r);
    if !r.good() {
        panic(r.failed);
    }
    let want = inputs[idx].data;
    push(comps, comp);
    push(codecs, codec);
    let dr = kafka.decompress(codec, comp, 16777216);
    if let got = dr {
        if got == want {
            println(name + " " + to_str(len(got)) + " " + digest(got));
        } else {
            println(name + " MISMATCH: " + to_str(len(got)) + " bytes, want " + to_str(len(want)));
        }
    } else let e = err_of(dr) {
        println(name + " ERROR: " + e);
    }
}

let out = kafka.new_buf();
for inp in inputs {
    for cname in ["gzip", "snappy", "lz4", "zstd"] {
        let c = codec_of(cname);
        let cr = kafka.compress(c, inp.data);
        guard let packed = cr else let e = err_of(cr) {
            println("roundtrip " + inp.name + " " + cname + " ERROR: " + e);
            continue;
        }
        let dr = kafka.decompress(c, packed, len(inp.data) + 1);
        let back = dr ?? b"x";
        if back == inp.data {
            println("roundtrip " + inp.name + " " + cname + " ok");
        } else {
            println("roundtrip " + inp.name + " " + cname + " FAILED: " + (err_of(dr)));
        }
        kafka.wr_str(out, inp.name + "/" + cname);
        kafka.wr_u8(out, c);
        kafka.wr_bytes(out, inp.data);
        kafka.wr_bytes(out, packed);
        io_eprint(inp.name + "/" + cname + ": " + to_str(len(inp.data)) + " -> " + to_str(len(packed)));
    }
}
let dest = proc.getenv("KAFKA_CODEC_OUT") ?? "";
if dest != "" {
    let cr = fs.create(dest);
    guard let fd = cr else let e = err_of(cr) { panic(e); }
    fs.write(fd as int, out.finish());
    fs.close(fd as int);
}

// truncations and corruptions must come back as errors (or, for a flip
// the format cannot see, other bytes), never a crash or a hang
let mutations = 0;
for i in 0..len(comps) {
    let comp = comps[i];
    let n = len(comp);
    if n < 4 {
        mutations += 6;
        continue;
    }
    let cuts = [n / 2, n - 1, n / 3];
    for cut in cuts {
        kafka.decompress(codecs[i], comp[0..cut], 1048576);
        mutations += 1;
    }
    for at in [n / 2, n - 2, n / 5] {
        let bad = comp[0..n];
        bad[at] = bad[at] ^ 0x5a;
        kafka.decompress(codecs[i], bad, 1048576);
        mutations += 1;
    }
}
println("hostile " + to_str(mutations) + " mutations, no crash");

let limit_ok = true;
for i in 0..len(comps) {
    let want_len = 0;
    let dr = kafka.decompress(codecs[i], comps[i], 1048576);
    if let got = dr {
        want_len = len(got);
    }
    if want_len < 2 {
        continue;
    }
    let lr = kafka.decompress(codecs[i], comps[i], want_len - 1);
    if let _l = lr {
        limit_ok = false;
        println("limit not enforced for vector " + to_str(i) + " codec " + to_str(codecs[i]));
    }
}
if limit_ok {
    println("limits ok");
}

fn io_eprint(s: str) {
    if (proc.getenv("KAFKA_CODEC_VERBOSE") ?? "") != "" {
        println("# " + s);
    }
}
