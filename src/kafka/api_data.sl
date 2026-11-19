// Produce v9, Fetch v12 and ListOffsets v6.

// ---- Produce --------------------------------------------------------------------

// One partition's batch (or batches) in a produce request.
pub gc struct ProducePart {
    partition: int,
    records: Buf,
}

pub gc struct ProduceTopic {
    topic: str,
    parts: [ProducePart],
}

pub fn enc_produce(w: Buf, txn_id: opt[str], acks: int, timeout_ms: int, topics: [ProduceTopic]) {
    enc_produce_v(w, 9, txn_id, acks, timeout_ms, topics);
}

// v7 (classic) or v9 (flexible).
pub fn enc_produce_v(w: Buf, v: int, txn_id: opt[str], acks: int, timeout_ms: int, topics: [ProduceTopic]) {
    let flex = v >= 9;
    wr_ns(w, flex, txn_id);
    wr_i16(w, acks);
    wr_i32(w, timeout_ms);
    wr_n(w, flex, len(topics));
    for t in topics {
        wr_s(w, flex, t.topic);
        wr_n(w, flex, len(t.parts));
        for p in t.parts {
            wr_i32(w, p.partition);
            if flex {
                wr_uvarint(w, p.records.size() + 1);
            } else {
                wr_i32(w, p.records.size());
            }
            wr_buf(w, p.records);
            wr_t(w, flex);
        }
        wr_t(w, flex);
    }
    wr_t(w, flex);
}

pub gc struct RecordError {
    batch_index: int,
    message: str,
}

pub gc struct ProducedPart {
    topic: str,
    partition: int,
    error_code: int,
    base_offset: int,
    log_append_time: int,
    log_start_offset: int,
    record_errors: [RecordError],
    error_message: str,
}

pub gc struct ProduceResp {
    parts: [ProducedPart],
    throttle_ms: int,
}

pub fn dec_produce(r: Reader) -> result[ProduceResp, str] {
    return dec_produce_v(r, 9);
}

pub fn dec_produce_v(r: Reader, v: int) -> result[ProduceResp, str] {
    let flex = v >= 9;
    let out = ProduceResp { parts: [], throttle_ms: 0 };
    let nt = rd_n(r, flex);
    for k in 0..nt {
        let topic = rd_s(r, flex);
        let np = rd_n(r, flex);
        for j in 0..np {
            let p = ProducedPart { topic: topic, partition: rd_i32(r), error_code: rd_i16(r), base_offset: rd_i64(r),
                                   log_append_time: rd_i64(r), log_start_offset: rd_i64(r), record_errors: [],
                                   error_message: "" };
            if v >= 8 {
                let ne = rd_n(r, flex);
                for e in 0..ne {
                    push(p.record_errors, RecordError { batch_index: rd_i32(r), message: rd_ns(r, flex) ?? "" });
                    rd_t(r, flex);
                }
                p.error_message = rd_ns(r, flex) ?? "";
            }
            rd_t(r, flex);
            push(out.parts, p);
        }
        rd_t(r, flex);
    }
    out.throttle_ms = rd_i32(r);
    rd_t(r, flex);
    let dr = done(r, "Produce");
    guard let _d = dr else let e = err_of(dr) { return err(e); }
    return ok(out);
}

// ---- Fetch ----------------------------------------------------------------------

pub let READ_UNCOMMITTED = 0;
pub let READ_COMMITTED = 1;

pub gc struct FetchPart {
    partition: int,
    // -1: not checked
    leader_epoch: int,
    offset: int,
    last_fetched_epoch: int,
    max_bytes: int,
}

pub gc struct FetchTopic {
    topic: str,
    parts: [FetchPart],
}

pub gc struct Forgotten {
    topic: str,
    partitions: [int],
}

pub gc struct FetchReq {
    max_wait_ms: int,
    min_bytes: int,
    max_bytes: int,
    isolation: int,
    session_id: int,
    session_epoch: int,
    topics: [FetchTopic],
    forgotten: [Forgotten],
    rack: str,
}

pub fn enc_fetch(w: Buf, f: FetchReq) {
    enc_fetch_v(w, 12, f);
}

// v11 (classic) or v12 (flexible, with the last fetched epoch).
pub fn enc_fetch_v(w: Buf, v: int, f: FetchReq) {
    let flex = v >= 12;
    wr_i32(w, -1);
    wr_i32(w, f.max_wait_ms);
    wr_i32(w, f.min_bytes);
    wr_i32(w, f.max_bytes);
    wr_u8(w, f.isolation);
    wr_i32(w, f.session_id);
    wr_i32(w, f.session_epoch);
    wr_n(w, flex, len(f.topics));
    for t in f.topics {
        wr_s(w, flex, t.topic);
        wr_n(w, flex, len(t.parts));
        for p in t.parts {
            wr_i32(w, p.partition);
            wr_i32(w, p.leader_epoch);
            wr_i64(w, p.offset);
            if flex {
                wr_i32(w, p.last_fetched_epoch);
            }
            wr_i64(w, -1);
            wr_i32(w, p.max_bytes);
            wr_t(w, flex);
        }
        wr_t(w, flex);
    }
    wr_n(w, flex, len(f.forgotten));
    for g in f.forgotten {
        wr_s(w, flex, g.topic);
        wr_i32s(w, flex, g.partitions);
        wr_t(w, flex);
    }
    wr_s(w, flex, f.rack);
    wr_t(w, flex);
}

pub gc struct Aborted {
    producer_id: int,
    first_offset: int,
}

// A partition's answer. Its records are frame[from..to] of the response
// (no copy); the consumer decodes batches from there.
pub gc struct FetchedPart {
    topic: str,
    partition: int,
    error_code: int,
    high_watermark: int,
    last_stable_offset: int,
    log_start_offset: int,
    aborted: opt[[Aborted]],
    preferred_replica: int,
    records_from: int,
    records_to: int,
}

pub gc struct FetchResp {
    throttle_ms: int,
    error_code: int,
    session_id: int,
    parts: [FetchedPart],
    // the response the record ranges index into
    frame: bytes,
}

pub fn dec_fetch(r: Reader) -> result[FetchResp, str] {
    return dec_fetch_v(r, 12);
}

pub fn dec_fetch_v(r: Reader, v: int) -> result[FetchResp, str] {
    let flex = v >= 12;
    let out = FetchResp { throttle_ms: rd_i32(r), error_code: rd_i16(r), session_id: rd_i32(r), parts: [], frame: r.b };
    let nt = rd_n(r, flex);
    for k in 0..nt {
        let topic = rd_s(r, flex);
        let np = rd_n(r, flex);
        for j in 0..np {
            let p = FetchedPart { topic: topic, partition: rd_i32(r), error_code: rd_i16(r), high_watermark: rd_i64(r),
                                  last_stable_offset: rd_i64(r), log_start_offset: rd_i64(r), aborted: none,
                                  preferred_replica: -1, records_from: 0, records_to: 0 };
            let na = rd_nn(r, flex);
            if na >= 0 {
                let list: [Aborted] = [];
                for a in 0..na {
                    push(list, Aborted { producer_id: rd_i64(r), first_offset: rd_i64(r) });
                    rd_t(r, flex);
                }
                p.aborted = some(list);
            }
            p.preferred_replica = rd_i32(r);
            let size = 0;
            if flex {
                size = rd_uvarint(r) - 1;
            } else {
                size = rd_i32(r);
            }
            if size > 0 {
                if size > r.left() {
                    fail_read(r, "records longer than their response");
                } else {
                    p.records_from = r.pos;
                    p.records_to = r.pos + size;
                    r.pos += size;
                }
            }
            rd_t(r, flex);
            push(out.parts, p);
        }
        rd_t(r, flex);
    }
    rd_t(r, flex);
    let dr = done(r, "Fetch");
    guard let _d = dr else let e = err_of(dr) { return err(e); }
    return ok(out);
}

// ---- ListOffsets ------------------------------------------------------------------

pub let OFFSET_LATEST = -1;
pub let OFFSET_EARLIEST = -2;

pub gc struct OffsetQuery {
    topic: str,
    partition: int,
    leader_epoch: int,
    // a timestamp (ms), OFFSET_LATEST or OFFSET_EARLIEST
    timestamp: int,
}

pub fn enc_list_offsets(w: Buf, isolation: int, qs: [OffsetQuery]) {
    enc_list_offsets_v(w, 6, isolation, qs);
}

// v4 (classic) or v6 (flexible).
pub fn enc_list_offsets_v(w: Buf, v: int, isolation: int, qs: [OffsetQuery]) {
    let flex = v >= 6;
    wr_i32(w, -1);
    wr_u8(w, isolation);
    let topics = group_by_topic_q(qs);
    wr_n(w, flex, len(topics));
    for t in topics {
        wr_s(w, flex, t[0].topic);
        wr_n(w, flex, len(t));
        for q in t {
            wr_i32(w, q.partition);
            wr_i32(w, q.leader_epoch);
            wr_i64(w, q.timestamp);
            wr_t(w, flex);
        }
        wr_t(w, flex);
    }
    wr_t(w, flex);
}

fn group_by_topic_q(qs: [OffsetQuery]) -> [[OffsetQuery]] {
    let out: [[OffsetQuery]] = [];
    let at: map[str]int = {};
    for q in qs {
        if !has(at, q.topic) {
            at[q.topic] = len(out);
            let fresh: [OffsetQuery] = [];
            push(out, fresh);
        }
        push(out[at[q.topic]], q);
    }
    return out;
}

pub gc struct ListedOffset {
    topic: str,
    partition: int,
    error_code: int,
    timestamp: int,
    offset: int,
    leader_epoch: int,
}

pub fn dec_list_offsets(r: Reader) -> result[[ListedOffset], str] {
    return dec_list_offsets_v(r, 6);
}

pub fn dec_list_offsets_v(r: Reader, v: int) -> result[[ListedOffset], str] {
    let flex = v >= 6;
    rd_i32(r);
    let out: [ListedOffset] = [];
    let nt = rd_n(r, flex);
    for k in 0..nt {
        let topic = rd_s(r, flex);
        let np = rd_n(r, flex);
        for j in 0..np {
            push(out, ListedOffset { topic: topic, partition: rd_i32(r), error_code: rd_i16(r), timestamp: rd_i64(r),
                                     offset: rd_i64(r), leader_epoch: rd_i32(r) });
            rd_t(r, flex);
        }
        rd_t(r, flex);
    }
    rd_t(r, flex);
    let dr = done(r, "ListOffsets");
    guard let _d = dr else let e = err_of(dr) { return err(e); }
    return ok(out);
}
