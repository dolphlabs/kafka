import "strings";

fn test_buf_keeps_long_values_as_parts() {
    let w = new_buf();
    wr_i32(w, 1);
    let big = strings.bytes_zero(1000);
    big[999] = 7;
    wr_raw(w, big);
    wr_i16(w, 2);
    assert(w.size() == 1006, "size " + to_str(w.size()));
    let parts = w.pieces();
    assert(len(parts) == 3, "parts " + to_str(len(parts)));
    let all = w.finish();
    assert(len(all) == 1006);
    assert(all[3] == 1 && all[1003] == 7 && all[1005] == 2);
    // finish again: the same bytes
    assert(w.finish() == all);
}

fn test_buf_appends_without_copying() {
    let inner = new_buf();
    wr_str(inner, "topic");
    let w = new_buf();
    wr_i32(w, inner.size());
    wr_buf(w, inner);
    wr_u8(w, 9);
    let all = w.finish();
    assert(len(all) == 4 + 7 + 1);
    assert(all[3] == 7 && all[5] == 5 && all[11] == 9);
}

fn test_buf_many_small_writes() {
    let w = new_buf();
    for i in 0..100000 {
        wr_u8(w, i);
    }
    let all = w.finish();
    assert(len(all) == 100000);
    assert(all[99999] == (99999 & 255));
}

fn test_strings_round_trip() {
    let w = new_buf();
    wr_str(w, "classic");
    wr_nstr(w, none);
    wr_nstr(w, some(""));
    wr_cstr(w, "compact");
    wr_cnstr(w, none);
    wr_bytes(w, b"\x00\x01");
    wr_nbytes(w, none);
    wr_cbytes(w, b"");
    wr_cnbytes(w, none);
    wr_clen(w, 3);
    wr_tags(w);
    wr_uuid(w, b"");
    let b = w.finish();
    let r = reader(b, 0, len(b));
    assert(rd_str(r) == "classic");
    assert((rd_nstr(r) ?? "x") == "x");
    assert((rd_nstr(r) ?? "x") == "");
    assert(rd_cstr(r) == "compact");
    assert((rd_cnstr(r) ?? "null") == "null");
    assert(rd_bytes(r) == b"\x00\x01");
    assert((rd_nbytes(r) ?? b"null") == b"null");
    assert(rd_cbytes(r) == b"");
    assert((rd_cnbytes(r) ?? b"null") == b"null");
    // 3 elements are claimed; only 17 bytes follow, so it is allowed
    assert(rd_clen(r) == 3);
    rd_tags(r);
    assert(len(rd_uuid(r)) == 16);
    assert(r.good() && r.left() == 0, r.failed);
}

fn test_reader_failure_sticks() {
    let b = b"\x00\x05ab";
    let r = reader(b, 0, len(b));
    assert(rd_str(r) == "");
    assert(!r.good());
    assert(strings.contains(r.failed, "truncated"), r.failed);
    assert(rd_i32(r) == 0);
    assert(strings.contains(r.failed, "5 bytes needed"), r.failed);
}

fn test_array_lengths_are_bounded() {
    let w = new_buf();
    wr_i32(w, 1000000);
    wr_i32(w, 0);
    let b = w.finish();
    let r = reader(b, 0, len(b));
    assert(rd_len(r) == 0);
    assert(strings.contains(r.failed, "an array of 1000000 elements in 4 bytes"), r.failed);
    let w2 = new_buf();
    wr_uvarint(w2, 4294967295);
    let b2 = w2.finish();
    let r2 = reader(b2, 0, len(b2));
    assert(rd_clen(r2) == 0 && !r2.good());
}

fn test_varint_too_long() {
    let b = b"\xff\xff\xff\xff\xff\x01";
    let r = reader(b, 0, len(b));
    rd_uvarint(r);
    assert(strings.contains(r.failed, "longer than 5 bytes"), r.failed);
}

fn test_tagged_fields() {
    let w = new_buf();
    wr_uvarint(w, 2);
    wr_uvarint(w, 0);
    wr_uvarint(w, 3);
    wr_raw(w, b"abc");
    wr_uvarint(w, 5);
    wr_uvarint(w, 1);
    wr_raw(w, b"z");
    wr_i16(w, 77);
    let b = w.finish();
    let r = reader(b, 0, len(b));
    let tags = rd_tag_list(r);
    assert(len(tags) == 2 && tags[0].data == b"abc" && tags[1].tag == 5);
    assert(rd_i16(r) == 77);
    let r2 = reader(b, 0, len(b));
    rd_tags(r2);
    assert(rd_i16(r2) == 77);
}

fn test_crc_check_values() {
    let t = crc_tables();
    let s = to_bytes("123456789");
    assert(crc32c(t, s) == 0xe3069283, "crc32c " + to_str(crc32c(t, s)));
    assert(crc32_ieee(t, s, 0, len(s)) == 0xcbf43926);
    // in pieces, across the eight-byte steps
    let long = strings.bytes_zero(1000);
    for i in 0..1000 {
        long[i] = (i * 31) & 255;
    }
    let whole = crc32c(t, long);
    let c = crc32c_update(t, 0, long, 0, 13);
    c = crc32c_update(t, c, long, 13, 500);
    c = crc32c_update(t, c, long, 500, 1000);
    assert(c == whole);
}

fn test_batch_round_trip_with_large_values() {
    let t = crc_tables();
    let recs: [Record] = [];
    for i in 0..20 {
        let r = record("t", to_bytes("k" + to_str(i)), strings.bytes_zero(i * 100));
        r.timestamp = 1000 + i;
        add_header(r, "n", to_bytes(to_str(i)));
        push(recs, r);
    }
    let er = encode_batch(t, batch_spec(), recs, 5, 20);
    guard let w = er else let e = err_of(er) { panic(e); }
    let b = w.finish();
    let r = reader(b, 0, len(b));
    let br = read_batch(r, t, true);
    guard let got = br else let e = err_of(br) { panic(e); }
    guard let batch = got else { panic("no batch"); }
    assert(batch.count == 15 && batch.next_offset() == 15 && batch.max_timestamp == 1019);
    let mr = batch_messages(t, batch, "t", 3, 1000000);
    guard let ms = mr else let e = err_of(mr) { panic(e); }
    assert(len(ms) == 15);
    assert(ms[14].offset == 14 && ms[14].timestamp == 1019 && ms[14].partition == 3);
    assert(len(ms[14].value ?? b"") == 1900);
    assert((header(ms[2], "n") ?? b"") == b"7");
}

fn test_empty_batch_refused() {
    let recs: [Record] = [];
    let er = encode_batch(crc_tables(), batch_spec(), recs, 0, 0);
    assert(err_of(er) == "kafka: a batch needs at least one record");
}
