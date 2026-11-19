import "strings";

fn test_xxhash_check_values() {
    assert(xxh32(b"", 0, 0) == 0x02cc5d05);
    assert(xxh32(b"a", 0, 1) == 0x550d7456);
    // 0xd24ec4f1a98c6e5b, from Go's cespare/xxhash
    assert(xxh64(b"a", 0, 1) == (0xd24ec4f1a98c6e5b as int));
    assert(xxh64(b"", 0, 0) == (0xef46db3751d8e999 as int));
}

fn sample() -> bytes {
    let w = new_buf();
    for i in 0..2000 {
        wr_raw(w, to_bytes("{\"id\":" + to_str(i) + ",\"kind\":\"event\"}\n"));
    }
    return w.finish();
}

fn test_every_codec_round_trips() {
    let data = sample();
    for c in [CODEC_NONE, CODEC_GZIP, CODEC_SNAPPY, CODEC_LZ4, CODEC_ZSTD] {
        let cr = compress(c, data);
        guard let packed = cr else let e = err_of(cr) { panic(e); }
        if c != CODEC_NONE {
            assert(len(packed) < len(data) / 3, codec_name(c) + " " + to_str(len(packed)));
        }
        let dr = decompress(c, packed, len(data));
        guard let back = dr else let e = err_of(dr) { panic(codec_name(c) + ": " + e); }
        assert(back == data, codec_name(c));
    }
}

fn test_limits_refuse_bombs() {
    let zeros = strings.bytes_zero(1000000);
    for c in [CODEC_GZIP, CODEC_SNAPPY, CODEC_LZ4, CODEC_ZSTD] {
        let packed = compress(c, zeros) ?? b"";
        assert(len(packed) < 60000, codec_name(c) + " " + to_str(len(packed)));
        let dr = decompress(c, packed, 999999);
        assert(err_of(dr) != "", codec_name(c) + " decompressed past its limit");
    }
}

fn test_empty_and_tiny_inputs() {
    for c in [CODEC_SNAPPY, CODEC_LZ4, CODEC_ZSTD] {
        for d in [b"", b"x", b"abcabcabcabcabcabc"] {
            let packed = compress(c, d) ?? b"";
            let back = decompress(c, packed, 100) ?? b"?";
            assert(back == d, codec_name(c));
        }
    }
}

fn test_garbage_is_an_error() {
    let junk = to_bytes("this is not compressed by anything at all");
    for c in [CODEC_GZIP, CODEC_LZ4, CODEC_ZSTD] {
        assert(err_of(decompress(c, junk, 1000)) != "", codec_name(c));
    }
    assert(err_of(decompress(9, junk, 1000)) == "kafka: compression codec 9 is unknown");
}

fn test_snappy_reads_raw_and_framed() {
    let data = sample();
    let raw = snappy_encode_raw(data);
    let framed = snappy_encode(data);
    assert(framed[0] == 130 && raw[0] != 130);
    assert((snappy_decode(raw, len(data)) ?? b"") == data);
    assert((snappy_decode(framed, len(data)) ?? b"") == data);
}

fn test_compressed_batch_round_trip() {
    let t = crc_tables();
    let recs: [Record] = [];
    for i in 0..300 {
        let r = record("t", to_bytes("k" + to_str(i % 7)), to_bytes("value " + to_str(i) + " of a compressed batch"));
        r.timestamp = 5000 + i;
        push(recs, r);
    }
    for c in [CODEC_GZIP, CODEC_SNAPPY, CODEC_LZ4, CODEC_ZSTD] {
        let s = batch_spec();
        s.codec = c;
        let er = encode_batch(t, s, recs, 0, len(recs));
        guard let w = er else let e = err_of(er) { panic(e); }
        let b = w.finish();
        let r = reader(b, 0, len(b));
        let got = read_batch(r, t, true) ?? none;
        guard let batch = got else { panic("no batch"); }
        assert(batch.codec() == c);
        let ms = batch_messages(t, batch, "t", 0, 1048576) ?? [];
        assert(len(ms) == 300, codec_name(c));
        assert((ms[299].value ?? b"") == to_bytes("value 299 of a compressed batch"));
        assert(ms[299].timestamp == 5299);
    }
}
