// The producer: records batched per partition, sent by one task per
// producer, idempotent by default.
//
//     let pr = kafka.producer(cl, kafka.producer_options());
//     guard let p = pr else let e = err_of(pr) { panic(e); }
//     let sr = kafka.send(p, kafka.record("orders", to_bytes(id), body));   // result[Pending, str], at once
//     guard let pend = sr else let e = err_of(sr) { panic(e); }               // e.g. the buffer is full
//     let dr = kafka.wait(p, pend, deadline);                               // result[Produced, str]
//     kafka.flush(p, deadline);
//     kafka.close_producer(p, deadline);
//
// send appends to the partition's open batch and returns; a batch is sent
// when it is full (batch_bytes) or linger_ms old, or on flush. Each
// partition has at most one batch in flight, and up to max_in_flight
// produce requests share a broker's connection, so a partition's records
// arrive in order. With idempotence (the default, which needs acks=all)
// every batch carries the producer id, epoch and a sequence number:
// a retried batch the broker already has is not written twice.

import "time";

pub gc struct ProducerOptions {
    // -1 (all in-sync replicas), 1 (the leader) or 0 (no answer)
    acks: int,
    idempotent: bool,
    linger_ms: int,
    batch_bytes: int,
    // the most bytes of records queued and in flight; send fails past it
    buffer_bytes: int,
    compression: int,
    // how long a record may take from send to its acknowledgement,
    // retries included
    delivery_timeout_ms: int,
    // produce requests in flight per broker
    max_in_flight: int,
    // how long send may wait for a topic's metadata the first time
    max_block_ms: int,
    // "" for a producer without transactions (milestone 7)
    transactional_id: str,
    transaction_timeout_ms: int,
}

pub fn producer_options() -> ProducerOptions {
    return ProducerOptions { acks: -1, idempotent: true, linger_ms: 5, batch_bytes: 1048576, buffer_bytes: 67108864,
                             compression: CODEC_NONE, delivery_timeout_ms: 120000, max_in_flight: 5,
                             max_block_ms: 60000, transactional_id: "", transaction_timeout_ms: 60000 };
}

gc struct PBatch {
    topic: str,
    partition: int,
    recs: [Record],
    size: int,
    created: int,
    done: chan[bool],
    finished: bool,
    err: str,
    base_offset: int,
    log_append_time: int,
    // -1 until the sender assigns it
    base_seq: int,
    // picked for sending: send appends no more
    sealed: bool,
    attempts: int,
    retry_at: int,
    // the encoded batch, and the epoch it was encoded with
    wire: bytes,
    wire_epoch: int,
    wire_seq: int,
}

gc struct PartQ {
    topic: str,
    partition: int,
    batches: [PBatch],
    head: int,
    inflight: bool,
    seq: int,
}

// A record send accepted: its batch and place in it.
pub struct Pending {
    batch: PBatch,
    index: int,
}

pub gc struct Produced {
    topic: str,
    partition: int,
    // -1 with acks=0, or when a retry found the batch already written
    offset: int,
    // the record's timestamp, or the broker's with LogAppendTime
    timestamp: int,
}

pub gc struct Producer {
    cl: Client,
    opts: ProducerOptions,
    lock: mutex,
    queues: map[str]PartQ,
    order: [PartQ],
    buffered: int,
    pid: int,
    epoch: int,
    closing: bool,
    closed: bool,
    why: str,
    kick: chan[bool],
    flushing: int,
    sticky: map[str]int,
    rr: int,
    node_inflight: map[int]int,
    bump_epoch: bool,
    // transactions (txn.sl)
    txn: TxnState,
}

// A producer over the client's connections.
pub fn producer(cl: Client, o: ProducerOptions) -> result[Producer, str] {
    if o.acks != -1 && o.acks != 0 && o.acks != 1 {
        return err("kafka: acks must be -1 (all), 0 or 1");
    }
    if o.idempotent && o.acks != -1 {
        return err("kafka: an idempotent producer needs acks=-1 (all); set idempotent = false for acks 0 or 1");
    }
    if o.transactional_id != "" && !o.idempotent {
        return err("kafka: a transactional producer is idempotent");
    }
    if o.batch_bytes < 1024 || o.buffer_bytes < o.batch_bytes || o.max_in_flight < 1 || o.max_in_flight > 5 ||
       o.linger_ms < 0 || o.delivery_timeout_ms <= 0 {
        return err("kafka: producer options out of range (batch_bytes >= 1024, buffer_bytes >= batch_bytes, " +
                   "max_in_flight 1-5, linger_ms >= 0)");
    }
    if o.compression < CODEC_NONE || o.compression > CODEC_ZSTD {
        return err("kafka: compression is one of the CODEC_ constants");
    }
    let p = Producer { cl: cl, opts: o, lock: make_mutex(), queues: {}, order: [], buffered: 0, pid: -1, epoch: -1,
                       closing: false, closed: false, why: "", kick: make_chan(1), flushing: 0, sticky: {}, rr: 0,
                       node_inflight: {}, bump_epoch: false, txn: no_txn() };
    if o.idempotent {
        let ir = init_producer_id(p, after_ms(o.max_block_ms));
        guard let _i = ir else let e = err_of(ir) { return err(e); }
    }
    spawn sender(p);
    return ok(p);
}

// InitProducerId: a new id and epoch, or (bump) the next epoch of ours.
fn init_producer_id(p: Producer, deadline: until) -> result[bool, str] {
    let last = "";
    while true {
        let body = new_buf();
        let txn_id: opt[str] = none;
        if p.opts.transactional_id != "" {
            txn_id = some(p.opts.transactional_id);
        }
        enc_init_producer_id(body, txn_id, p.opts.transaction_timeout_ms, p.pid, p.epoch);
        let rr = txn_or_any_call(p, API_INIT_PRODUCER_ID, body, deadline);
        if let r = rr {
            let dr = dec_init_producer_id(r);
            guard let res = dr else let e = err_of(dr) { return err(e); }
            if res.error_code == 0 {
                p.pid = res.producer_id;
                p.epoch = res.epoch;
                return ok(true);
            }
            last = kerr("InitProducerId", res.error_code, "");
            if res.error_code == ERR_NOT_COORDINATOR || res.error_code == ERR_COORDINATOR_NOT_AVAILABLE {
                forget_coordinator(p.cl, p.opts.transactional_id, COORDINATOR_TRANSACTION);
            } else if !is_retriable(res.error_code) {
                return err(last);
            }
        } else let e = err_of(rr) {
            last = e;
        }
        if until_hit(deadline) {
            return err(last);
        }
        sleep_ms(p.cl.cfg.retry_backoff_ms);
    }
    return err(last);
}

fn now_ms() -> int {
    return time.wall() / 1000000;
}

fn record_size(r: Record) -> int {
    let n = 24 + len(r.key ?? b"") + len(r.value ?? b"");
    for h in r.headers {
        n += 8 + len(h.key) + len(h.value ?? b"");
    }
    return n;
}

fn queue_for(p: Producer, topic: str, partition: int) -> PartQ {
    let key = topic + "/" + to_str(partition);
    if has(p.queues, key) {
        return p.queues[key];
    }
    let q = PartQ { topic: topic, partition: partition, batches: [], head: 0, inflight: false, seq: 0 };
    p.queues[key] = q;
    push(p.order, q);
    return q;
}

fn open_batch(q: PartQ) -> opt[PBatch] {
    if q.head < len(q.batches) {
        let b = q.batches[len(q.batches) - 1];
        if !b.sealed {
            return some(b);
        }
    }
    return none;
}

// Queues rec and returns at once. A timestamp of -1 becomes now; a
// partition of -1 is chosen from the key (murmur2) or, without a key, a
// partition kept until its batch fills.
pub fn send(p: Producer, rec: Record) -> result[Pending, str] {
    if p.closing || p.closed {
        return err("kafka: the producer is closed");
    }
    if p.txn.fatal != "" {
        return err(p.txn.fatal);
    }
    if p.opts.transactional_id != "" && !p.txn.in_txn {
        return err("kafka: a transactional producer sends inside begin_transaction and commit/abort");
    }
    if rec.timestamp < 0 {
        rec.timestamp = now_ms();
    }
    let size = record_size(rec);
    let n = partition_count(p.cl, rec.topic);
    if n == 0 || rec.partition >= n {
        if n == 0 {
            let ur = use_topics(p.cl, [rec.topic], after_ms(p.opts.max_block_ms));
            guard let _u = ur else let e = err_of(ur) { return err(e); }
            n = partition_count(p.cl, rec.topic);
            if n == 0 {
                let ti = topic_info(p.cl, rec.topic);
                if let t = ti {
                    if t.error_code != 0 {
                        return err(kerr("produce to " + rec.topic, t.error_code, ""));
                    }
                }
                return err("kafka: topic " + rec.topic + " has no partitions");
            }
        }
        if rec.partition >= n {
            return err("kafka: topic " + rec.topic + " has no partition " + to_str(rec.partition));
        }
    }
    let partition = rec.partition;
    if partition < 0 {
        if let k = rec.key {
            partition = key_partition(k, n);
        }
    }
    mutex_lock(p.lock);
    if p.buffered + size > p.opts.buffer_bytes {
        mutex_unlock(p.lock);
        return err("kafka: the producer's buffer is full (" + to_str(p.opts.buffer_bytes) +
                   " bytes): records are produced more slowly than they are sent");
    }
    if partition < 0 {
        partition = sticky_partition(p, rec.topic, n, size);
    }
    if p.opts.transactional_id != "" {
        note_txn_partition(p, rec.topic, partition);
    }
    let q = queue_for(p, rec.topic, partition);
    let ob = open_batch(q);
    let b = PBatch { topic: rec.topic, partition: partition, recs: [], size: 0, created: now_ns(), done: make_chan(1),
                     finished: false, err: "", base_offset: -1, log_append_time: -1, base_seq: -1, sealed: false,
                     attempts: 0, retry_at: 0, wire: b"", wire_epoch: -2, wire_seq: -2 };
    let fresh = true;
    if let open = ob {
        if open.size + size <= p.opts.batch_bytes {
            b = open;
            fresh = false;
        } else {
            open.sealed = true;
        }
    }
    if fresh {
        push(q.batches, b);
    }
    push(b.recs, rec);
    b.size += size;
    p.buffered += size;
    let idx = len(b.recs) - 1;
    let full = b.size >= p.opts.batch_bytes;
    mutex_unlock(p.lock);
    if fresh || full || p.opts.linger_ms == 0 {
        nudge(p);
    }
    return ok(Pending { batch: b, index: idx });
}

// Without a key: stay on one partition until its open batch would
// overflow, then move to the next (so batches fill instead of a record
// going to every partition).
fn sticky_partition(p: Producer, topic: str, n: int, size: int) -> int {
    if !has(p.sticky, topic) {
        p.rr += 1;
        p.sticky[topic] = p.rr % n;
    }
    let cur = p.sticky[topic];
    if cur >= n {
        cur = cur % n;
    }
    let key = topic + "/" + to_str(cur);
    if has(p.queues, key) {
        let ob = open_batch(p.queues[key]);
        if let b = ob {
            if b.size + size > p.opts.batch_bytes {
                p.rr += 1;
                cur = p.rr % n;
            }
        }
    }
    p.sticky[topic] = cur;
    return cur;
}

fn nudge(p: Producer) {
    select {
        case chan_send(p.kick, true) { }
        default { }
    }
}

// send, then wait for the result.
pub fn produce(p: Producer, rec: Record, deadline: until) -> result[Produced, str] {
    let sr = send(p, rec);
    guard let pend = sr else let e = err_of(sr) { return err(e); }
    return wait(p, pend, deadline);
}

// The result of a record send accepted. A deadline that passes first is
// an error, but the record is still being delivered.
pub fn wait(p: Producer, pend: Pending, deadline: until) -> result[Produced, str] {
    let b = pend.batch;
    if !b.finished {
        let w = add_waiter(p.cl.clock, deadline, until_never());
        select {
            case let _d = chan_recv(b.done) { }
            case let _t = chan_recv(w.wake) { }
        }
        w.done = true;
    }
    if !b.finished {
        return err("kafka: waiting for a produce result timed out (the record is still being delivered)");
    }
    if b.err != "" {
        return err(b.err);
    }
    let rec = b.recs[pend.index];
    let off = -1;
    if b.base_offset >= 0 {
        off = b.base_offset + pend.index;
    }
    let ts = rec.timestamp;
    if b.log_append_time >= 0 {
        ts = b.log_append_time;
    }
    return ok(Produced { topic: b.topic, partition: b.partition, offset: off, timestamp: ts });
}

// Whether the record's result is in.
pub fn is_done(pend: Pending) -> bool {
    return pend.batch.finished;
}

// Sends everything queued now and waits for it; an error names how many
// records failed and the first reason.
pub fn flush(p: Producer, deadline: until) -> result[bool, str] {
    mutex_lock(p.lock);
    p.flushing += 1;
    let open: [PBatch] = [];
    for q in p.order {
        for i in q.head..len(q.batches) {
            push(open, q.batches[i]);
        }
    }
    mutex_unlock(p.lock);
    nudge(p);
    let failed = 0;
    let first = "";
    let timed_out = false;
    for b in open {
        if !b.finished {
            let w = add_waiter(p.cl.clock, deadline, until_never());
            select {
                case let _d = chan_recv(b.done) { }
                case let _t = chan_recv(w.wake) { }
            }
            w.done = true;
        }
        if !b.finished {
            timed_out = true;
            break;
        }
        if b.err != "" {
            failed += len(b.recs);
            if first == "" {
                first = b.err;
            }
        }
    }
    mutex_lock(p.lock);
    p.flushing -= 1;
    mutex_unlock(p.lock);
    if timed_out {
        return err("kafka: flush timed out with records still in flight");
    }
    if failed > 0 {
        return err("kafka: " + to_str(failed) + " records failed; the first: " + first);
    }
    return ok(true);
}

// Flushes, then stops the sender. Records still queued after deadline
// fail with "the producer is closed".
pub fn close_producer(p: Producer, deadline: until) -> result[bool, str] {
    if p.closed {
        return ok(true);
    }
    p.closing = true;
    let fr = flush(p, deadline);
    mutex_lock(p.lock);
    p.closed = true;
    let left: [PBatch] = [];
    for q in p.order {
        for i in q.head..len(q.batches) {
            push(left, q.batches[i]);
        }
    }
    mutex_unlock(p.lock);
    for b in left {
        finish_batch(p, b, -1, -1, "kafka: the producer is closed");
    }
    nudge(p);
    return fr;
}

// ---- the sender -------------------------------------------------------------------

fn sender(p: Producer) {
    while !p.closed {
        let wake_in = drain(p);
        if p.closed {
            break;
        }
        if wake_in < 0 {
            let k = chan_recv(p.kick);
            if let _v = k {
                continue;
            }
            break;
        }
        // something waits on linger or a retry backoff: sleep until then
        // or until a send or a response kicks
        let w = add_waiter(p.cl.clock, until_of(time.mono() + wake_in), until_never());
        select {
            case let _k = chan_recv(p.kick) { }
            case let _t = chan_recv(w.wake) { }
        }
        w.done = true;
    }
}

gc struct NodeBatches {
    node: int,
    batches: [PBatch],
}

// Sends every batch that is ready. Returns the nanoseconds until the next
// one will be (-1: none is waiting).
fn drain(p: Producer) -> int {
    if p.bump_epoch && p.opts.idempotent {
        bump(p);
    }
    let now = now_ns();
    let linger = p.opts.linger_ms * MS;
    let next = -1;
    let groups: map[int]NodeBatches = {};
    let expired: [PBatch] = [];
    mutex_lock(p.lock);
    for q in p.order {
        if q.inflight || q.head >= len(q.batches) {
            continue;
        }
        let b = q.batches[q.head];
        if now - b.created > p.opts.delivery_timeout_ms * MS {
            push(expired, b);
            continue;
        }
        if b.retry_at > now {
            next = sooner(next, b.retry_at - now);
            continue;
        }
        let ready = b.sealed || b.size >= p.opts.batch_bytes || now - b.created >= linger || p.flushing > 0 ||
                    p.closing || len(q.batches) - q.head > 1 || p.txn.committing;
        if !ready {
            next = sooner(next, b.created + linger - now);
            continue;
        }
        let leader = leader_of(p.cl, q.topic, q.partition);
        if leader < 0 {
            metadata_stale(p.cl);
            b.retry_at = now + p.cl.cfg.retry_backoff_ms * MS;
            next = sooner(next, p.cl.cfg.retry_backoff_ms * MS);
            continue;
        }
        let busy = 0;
        if has(p.node_inflight, leader) {
            busy = p.node_inflight[leader];
        }
        if has(groups, leader) {
            busy += 1;
        }
        if busy >= p.opts.max_in_flight {
            continue;
        }
        if p.opts.transactional_id != "" && !txn_partition_added(p, q.topic, q.partition) {
            continue;
        }
        if b.base_seq < 0 && p.opts.idempotent {
            b.base_seq = q.seq;
            q.seq = (q.seq + len(b.recs)) % 2147483648;
        }
        b.sealed = true;
        q.inflight = true;
        if !has(groups, leader) {
            groups[leader] = NodeBatches { node: leader, batches: [] };
        }
        push(groups[leader].batches, b);
    }
    for node, g in groups {
        let cur = 0;
        if has(p.node_inflight, node) {
            cur = p.node_inflight[node];
        }
        p.node_inflight[node] = cur + 1;
    }
    mutex_unlock(p.lock);
    for b in expired {
        finish_batch(p, b, -1, -1, "kafka: produce to " + b.topic + "/" + to_str(b.partition) + " timed out after " +
                                   to_str(p.opts.delivery_timeout_ms) + " ms (delivery_timeout_ms)");
    }
    for node, g in groups {
        spawn produce_request(p, g);
    }
    if len(expired) > 0 {
        nudge(p);
    }
    return next;
}

fn sooner(a: int, b: int) -> int {
    let v = b;
    if v < MS {
        v = MS;
    }
    if a < 0 || v < a {
        return v;
    }
    return a;
}

fn encode_for(p: Producer, b: PBatch) -> result[bytes, str] {
    if len(b.wire) > 0 && b.wire_epoch == p.epoch && b.wire_seq == b.base_seq {
        return ok(b.wire);
    }
    let s = batch_spec();
    s.producer_id = p.pid;
    s.producer_epoch = p.epoch;
    s.base_sequence = b.base_seq;
    s.codec = p.opts.compression;
    s.transactional = p.opts.transactional_id != "";
    let er = encode_batch(p.cl.crc, s, b.recs, 0, len(b.recs));
    guard let w = er else let e = err_of(er) { return err(e); }
    b.wire = w.finish();
    b.wire_epoch = p.epoch;
    b.wire_seq = b.base_seq;
    return ok(b.wire);
}

fn produce_request(p: Producer, g: NodeBatches) {
    let topics: [ProduceTopic] = [];
    let at: map[str]int = {};
    let sent: [PBatch] = [];
    for b in g.batches {
        let er = encode_for(p, b);
        guard let wire = er else let e = err_of(er) {
            finish_batch(p, b, -1, -1, e);
            release(p, b);
            continue;
        }
        if !has(at, b.topic) {
            at[b.topic] = len(topics);
            push(topics, ProduceTopic { topic: b.topic, parts: [] });
        }
        let rb = new_buf();
        wr_raw(rb, wire);
        push(topics[at[b.topic]].parts, ProducePart { partition: b.partition, records: rb });
        push(sent, b);
    }
    if len(sent) > 0 {
        let txn_id: opt[str] = none;
        if p.opts.transactional_id != "" {
            txn_id = some(p.opts.transactional_id);
        }
        let body = new_buf();
        enc_produce(body, txn_id, p.opts.acks, p.cl.cfg.request_timeout_ms, topics);
        if p.opts.acks == 0 {
            let cr = conn_to(p.cl, g.node, "produce", after_ms(p.cl.cfg.request_timeout_ms));
            let ok_sent = false;
            if let c = cr {
                let sr = send_request(c, API_PRODUCE, body, false);
                if let _s = sr {
                    ok_sent = true;
                }
            }
            for b in sent {
                if ok_sent {
                    finish_batch(p, b, -1, -1, "");
                } else {
                    retry_batch(p, b, "kafka: no connection to broker " + to_str(g.node));
                }
                release(p, b);
            }
        } else {
            let rr = call_node(p.cl, g.node, "produce", API_PRODUCE, body, after_ms(p.cl.cfg.request_timeout_ms));
            if let r = rr {
                let dr = dec_produce(r);
                if let res = dr {
                    handle_produce(p, sent, res);
                } else let e = err_of(dr) {
                    for b in sent {
                        retry_batch(p, b, e);
                        release(p, b);
                    }
                }
            } else let e = err_of(rr) {
                metadata_stale(p.cl);
                for b in sent {
                    retry_batch(p, b, e);
                    release(p, b);
                }
            }
        }
    }
    mutex_lock(p.lock);
    p.node_inflight[g.node] = p.node_inflight[g.node] - 1;
    mutex_unlock(p.lock);
    nudge(p);
}

fn handle_produce(p: Producer, sent: [PBatch], res: ProduceResp) {
    let by: map[str]ProducedPart = {};
    for x in res.parts {
        by[x.topic + "/" + to_str(x.partition)] = x;
    }
    for b in sent {
        let key = b.topic + "/" + to_str(b.partition);
        if !has(by, key) {
            retry_batch(p, b, "kafka: the produce response left out " + key);
            release(p, b);
            continue;
        }
        let x = by[key];
        let code = x.error_code;
        if code == 0 {
            finish_batch(p, b, x.base_offset, x.log_append_time, "");
        } else if code == ERR_DUPLICATE_SEQUENCE_NUMBER {
            // the broker has it from an earlier attempt
            finish_batch(p, b, -1, -1, "");
        } else if code == ERR_OUT_OF_ORDER_SEQUENCE_NUMBER || code == ERR_UNKNOWN_PRODUCER_ID ||
                  code == ERR_INVALID_PRODUCER_EPOCH {
            if p.opts.transactional_id != "" {
                txn_fatal(p, kerr("produce to " + key, code, x.error_message));
                finish_batch(p, b, -1, -1, p.txn.fatal);
            } else if code == ERR_INVALID_PRODUCER_EPOCH && b.wire_epoch != p.epoch {
                // sent with an epoch since replaced: send again with the new one
                b.base_seq = -1;
                retry_batch(p, b, kerr("produce to " + key, code, ""));
            } else {
                // the broker lost track of this producer's sequence: this
                // batch fails, and a new epoch restarts every sequence
                p.bump_epoch = true;
                finish_batch(p, b, -1, -1, kerr("produce to " + key, code, x.error_message));
            }
        } else if code == ERR_PRODUCER_FENCED || code == ERR_TRANSACTION_ABORTABLE {
            if p.opts.transactional_id != "" {
                txn_abortable(p, kerr("produce to " + key, code, x.error_message), code == ERR_PRODUCER_FENCED);
            }
            finish_batch(p, b, -1, -1, kerr("produce to " + key, code, x.error_message));
        } else if is_retriable(code) {
            if code == ERR_NOT_LEADER_OR_FOLLOWER || code == ERR_LEADER_NOT_AVAILABLE ||
               code == ERR_UNKNOWN_TOPIC_OR_PARTITION || code == ERR_FENCED_LEADER_EPOCH {
                metadata_stale(p.cl);
            }
            retry_batch(p, b, kerr("produce to " + key, code, x.error_message));
        } else {
            let msg = x.error_message;
            if msg == "" && len(x.record_errors) > 0 {
                msg = x.record_errors[0].message;
            }
            finish_batch(p, b, -1, -1, kerr("produce to " + key, code, msg));
        }
        release(p, b);
    }
}

// Sends b again after a backoff (the same sequence: an idempotent broker
// drops a copy it already has), or fails it past the delivery timeout.
fn retry_batch(p: Producer, b: PBatch, why: str) {
    let now = now_ns();
    if now - b.created > p.opts.delivery_timeout_ms * MS || p.closed {
        finish_batch(p, b, -1, -1, why);
        return;
    }
    b.attempts += 1;
    let backoff = p.cl.cfg.retry_backoff_ms;
    let k = b.attempts;
    while k > 1 && backoff < 1000 {
        backoff = backoff * 2;
        k -= 1;
    }
    b.retry_at = now + backoff * MS;
}

// A batch's request is over: its partition may send the next one, and a
// finished batch leaves the queue.
fn release(p: Producer, b: PBatch) {
    mutex_lock(p.lock);
    let key = b.topic + "/" + to_str(b.partition);
    if has(p.queues, key) {
        let q = p.queues[key];
        q.inflight = false;
        pop_finished(q);
    }
    mutex_unlock(p.lock);
}

fn pop_finished(q: PartQ) {
    while q.head < len(q.batches) && q.batches[q.head].finished {
        q.head += 1;
    }
    if q.head > 32 && q.head * 2 > len(q.batches) {
        let rest: [PBatch] = [];
        for i in q.head..len(q.batches) {
            push(rest, q.batches[i]);
        }
        q.batches = rest;
        q.head = 0;
    }
}

fn finish_batch(p: Producer, b: PBatch, base: int, lat: int, e: str) {
    mutex_lock(p.lock);
    if b.finished {
        mutex_unlock(p.lock);
        return;
    }
    b.finished = true;
    b.base_offset = base;
    b.log_append_time = lat;
    b.err = e;
    b.wire = b"";
    p.buffered -= b.size;
    let key = b.topic + "/" + to_str(b.partition);
    if has(p.queues, key) && !p.queues[key].inflight {
        pop_finished(p.queues[key]);
    }
    mutex_unlock(p.lock);
    if e != "" && p.opts.transactional_id != "" {
        txn_abortable(p, e, false);
    }
    chan_close(b.done);
}

// A new epoch after the broker lost our sequence: every partition's
// sequence starts over, and batches not yet acknowledged get new ones.
fn bump(p: Producer) {
    p.bump_epoch = false;
    let ir = init_producer_id(p, after_ms(p.cl.cfg.request_timeout_ms));
    guard let _i = ir else let e = err_of(ir) {
        p.bump_epoch = true;
        return;
    }
    mutex_lock(p.lock);
    for q in p.order {
        q.seq = 0;
        for i in q.head..len(q.batches) {
            let b = q.batches[i];
            if !b.finished && !q.inflight {
                b.base_seq = -1;
            }
        }
    }
    mutex_unlock(p.lock);
}

// The producer id and epoch (-1 without idempotence).
pub fn producer_id(p: Producer) -> int {
    return p.pid;
}

pub fn producer_epoch(p: Producer) -> int {
    return p.epoch;
}

// Bytes of records queued or in flight.
pub fn buffered_bytes(p: Producer) -> int {
    return p.buffered;
}
