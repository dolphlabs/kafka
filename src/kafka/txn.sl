// Transaction state a producer carries. Without a transactional_id it
// stays empty and InitProducerId goes to any broker; the transactional
// calls (begin, send offsets, commit, abort) build on it.

gc struct TxnState {
    in_txn: bool,
    committing: bool,
    // partitions written in the current transaction, and those the
    // coordinator has confirmed (AddPartitionsToTxn)
    parts: map[str]bool,
    added: map[str]bool,
    // set when the transaction cannot commit (a send failed): only abort
    abortable: str,
    // set when the producer cannot go on (fenced, or a sequence lost)
    fatal: str,
    // AddOffsetsToTxn was sent in this transaction
    offsets_added: bool,
}

fn no_txn() -> TxnState {
    return TxnState { in_txn: false, committing: false, parts: {}, added: {}, abortable: "", fatal: "", offsets_added: false };
}

// InitProducerId and the transaction calls go to the transaction
// coordinator; an idempotent producer's InitProducerId to any broker.
fn txn_or_any_call(p: Producer, key: int, body: Buf, deadline: until) -> result[Reader, str] {
    if p.opts.transactional_id == "" {
        return call_any(p.cl, key, body, deadline);
    }
    let nr = coordinator(p.cl, p.opts.transactional_id, COORDINATOR_TRANSACTION, deadline);
    guard let node = nr else let e = err_of(nr) { return err(e); }
    let rr = call_node(p.cl, node, "ctl", key, body, deadline);
    if let r = rr {
        return ok(r);
    } else let e = err_of(rr) {
        forget_coordinator(p.cl, p.opts.transactional_id, COORDINATOR_TRANSACTION);
        return err(e);
    }
}

// AddOffsetsToTxn at the version the coordinator takes (v1 on Redpanda):
// its error code.
fn add_offsets_call(p: Producer, group: str, deadline: until) -> result[int, str] {
    let nr = coordinator(p.cl, p.opts.transactional_id, COORDINATOR_TRANSACTION, deadline);
    guard let node = nr else let e = err_of(nr) { return err(e); }
    let cr = conn_to(p.cl, node, "ctl", deadline);
    guard let c = cr else let e = err_of(cr) { return err(e); }
    let v = conn_version(c, API_ADD_OFFSETS_TO_TXN);
    let body = new_buf();
    enc_add_offsets_to_txn_v(body, v, p.opts.transactional_id, p.pid, p.epoch, group);
    let rr = call_conn(c, API_ADD_OFFSETS_TO_TXN, body, deadline);
    guard let r = rr else let e = err_of(rr) {
        forget_coordinator(p.cl, p.opts.transactional_id, COORDINATOR_TRANSACTION);
        return err(e);
    }
    return dec_error_only_v(r, "AddOffsetsToTxn", v >= 3);
}

// Called under p.lock when send puts a record in topic/partition.
fn note_txn_partition(p: Producer, topic: str, partition: int) {
    p.txn.parts[topic + "/" + itoa(partition)] = true;
}

// Called under p.lock by the sender: a transactional batch waits until
// the coordinator has its partition.
fn txn_partition_added(p: Producer, topic: str, partition: int) -> bool {
    return has(p.txn.added, topic + "/" + itoa(partition));
}

fn txn_fatal(p: Producer, why: str) {
    if p.txn.fatal == "" {
        p.txn.fatal = why;
    }
    // the sender fails what is queued
    nudge(p);
}

fn txn_abortable(p: Producer, why: str, fenced: bool) {
    if fenced {
        txn_fatal(p, why);
    }
    if p.txn.abortable == "" {
        p.txn.abortable = why;
    }
}

// ---- the transactional API ------------------------------------------------------------
//
//     let o = kafka.producer_options();
//     o.transactional_id = "billing-1";                 // stable across restarts: fences the old instance
//     let p = kafka.producer(cl, o) ?? ...;
//     kafka.begin_transaction(p);
//     kafka.send(p, rec);  ...
//     kafka.send_offsets_to_transaction(p, consumer, deadline);   // exactly-once consume-transform-produce
//     let cr = kafka.commit_transaction(p, deadline);
//     guard let _c = cr else { kafka.abort_transaction(p, deadline); }
//
// Every partition a transaction writes is registered with the
// transaction coordinator (AddPartitionsToTxn) before its first batch is
// sent. Committing flushes, then EndTxn writes the commit markers; a
// read_committed consumer sees the records only after that. A second
// producer with the same transactional_id bumps the epoch and fences this
// one: its next call fails with PRODUCER_FENCED.

fn txn_check(p: Producer) -> result[bool, str] {
    if p.opts.transactional_id == "" {
        return err("kafka: this producer has no transactional_id");
    }
    if p.txn.fatal != "" {
        return err(p.txn.fatal);
    }
    return ok(true);
}

pub fn begin_transaction(p: Producer) -> result[bool, str] {
    let cr = txn_check(p);
    guard let _c = cr else let e = err_of(cr) { return err(e); }
    if p.txn.in_txn {
        return err("kafka: a transaction is already open");
    }
    mutex_lock(p.lock);
    p.txn.in_txn = true;
    p.txn.parts = {};
    p.txn.added = {};
    p.txn.abortable = "";
    mutex_unlock(p.lock);
    return ok(true);
}

// Called by the sender: registers the transaction's new partitions with
// the coordinator, so their batches can be sent.
fn add_txn_partitions(p: Producer) {
    mutex_lock(p.lock);
    let todo: [TopicParts] = [];
    let at: map[str]int = {};
    let keys: [str] = [];
    for key, _v in p.txn.parts {
        if has(p.txn.added, key) {
            continue;
        }
        let slash = rfind_slash(key);
        let topic = substr(key, 0, slash);
        let part = to_int(substr(key, slash + 1, len(key))) ?? 0;
        if !has(at, topic) {
            at[topic] = len(todo);
            push(todo, TopicParts { topic: topic, partitions: [] });
        }
        push(todo[at[topic]].partitions, part);
        push(keys, key);
    }
    mutex_unlock(p.lock);
    if len(todo) == 0 {
        return;
    }
    let body = new_buf();
    enc_add_partitions_to_txn(body, p.opts.transactional_id, p.pid, p.epoch, todo);
    let rr = txn_or_any_call(p, API_ADD_PARTITIONS_TO_TXN, body, after_ms(p.cl.cfg.request_timeout_ms));
    guard let r = rr else { return; }
    let dr = dec_part_errors(r, "AddPartitionsToTxn");
    guard let res = dr else let e = err_of(dr) { txn_abortable(p, e, false); return; }
    mutex_lock(p.lock);
    for x in res {
        let key = x.topic + "/" + itoa(x.partition);
        if x.error_code == 0 {
            p.txn.added[key] = true;
        } else if x.error_code == ERR_PRODUCER_FENCED || x.error_code == ERR_INVALID_PRODUCER_EPOCH ||
                  x.error_code == ERR_FENCED_INSTANCE_ID {
            mutex_unlock(p.lock);
            txn_fatal(p, kerr("add " + key + " to the transaction", x.error_code, ""));
            mutex_lock(p.lock);
        } else if x.error_code == ERR_NOT_COORDINATOR || x.error_code == ERR_COORDINATOR_NOT_AVAILABLE {
            forget_coordinator(p.cl, p.opts.transactional_id, COORDINATOR_TRANSACTION);
        } else if !is_retriable(x.error_code) && x.error_code != 55 {
            // 55 OPERATION_NOT_ATTEMPTED: another partition's error stopped it
            p.txn.abortable = kerr("add " + key + " to the transaction", x.error_code, "");
        }
    }
    mutex_unlock(p.lock);
}

fn rfind_slash(s: str) -> int {
    let b = to_bytes(s);
    let i = len(b) - 1;
    while i >= 0 && b[i] != 47 {
        i -= 1;
    }
    return i;
}

fn substr(s: str, from: int, to: int) -> str {
    return to_str(to_bytes(s)[from..to]);
}

// Adds the consumer's position (what poll returned) to the transaction:
// it commits with the records, or not at all.
pub fn send_offsets_to_transaction(p: Producer, c: Consumer, deadline: until) -> result[bool, str] {
    let cr = txn_check(p);
    guard let _c = cr else let e = err_of(cr) { return err(e); }
    if !p.txn.in_txn {
        return err("kafka: send_offsets_to_transaction outside a transaction");
    }
    if c.opts.group == "" {
        return err("kafka: the consumer has no group");
    }
    let cs: [OffsetToCommit] = [];
    mutex_lock(c.lock);
    for ps in c.order {
        if ps.consumed >= 0 {
            push(cs, OffsetToCommit { topic: ps.topic, partition: ps.partition, offset: ps.consumed, leader_epoch: -1,
                                      metadata: none });
        }
    }
    mutex_unlock(c.lock);
    if len(cs) == 0 {
        return ok(true);
    }
    let last = "";
    // AddOffsetsToTxn: the coordinator learns the group
    let added = false;
    while !added {
        let rr = add_offsets_call(p, c.opts.group, deadline);
        if let r = rr {
            let code = r;
            if code == 0 {
                added = true;
                p.txn.offsets_added = true;
                continue;
            }
            last = kerr("add offsets to the transaction", code, "");
            let fr = txn_code(p, code, last);
            guard let _f = fr else let e = err_of(fr) { return err(e); }
        } else let e = err_of(rr) {
            last = e;
        }
        if until_hit(deadline) {
            return err(last);
        }
        sleep_ms(p.cl.cfg.retry_backoff_ms);
    }
    // TxnOffsetCommit to the group coordinator, with the consumer's
    // generation (KIP-447: a zombie member's offsets are refused)
    while true {
        let body = new_buf();
        let generation = -1;
        let member = "";
        if len(c.subscribed) > 0 {
            generation = c.generation;
            member = c.member_id;
        }
        enc_txn_offset_commit(body, p.opts.transactional_id, c.opts.group, p.pid, p.epoch, generation, member,
                              instance_opt(c), cs);
        let rr = coordinator_call(c, API_TXN_OFFSET_COMMIT, body, deadline);
        if let r = rr {
            let dr = dec_part_errors(r, "TxnOffsetCommit");
            guard let res = dr else let e = err_of(dr) { return err(e); }
            let worst = 0;
            for x in res {
                if x.error_code != 0 {
                    worst = x.error_code;
                }
            }
            if worst == 0 {
                return ok(true);
            }
            last = kerr("commit offsets in the transaction", worst, "");
            if worst == ERR_NOT_COORDINATOR || worst == ERR_COORDINATOR_NOT_AVAILABLE {
                forget_coordinator(p.cl, c.opts.group, COORDINATOR_GROUP);
            } else if !is_retriable(worst) {
                txn_abortable(p, last, worst == ERR_PRODUCER_FENCED);
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

// What an error code from the transaction coordinator means: ok to retry,
// err to give up (fenced: for good).
fn txn_code(p: Producer, code: int, msg: str) -> result[bool, str] {
    if code == ERR_PRODUCER_FENCED || code == ERR_INVALID_PRODUCER_EPOCH || code == 53 {
        txn_fatal(p, msg);
        return err(msg);
    }
    if code == ERR_NOT_COORDINATOR || code == ERR_COORDINATOR_NOT_AVAILABLE {
        forget_coordinator(p.cl, p.opts.transactional_id, COORDINATOR_TRANSACTION);
        return ok(true);
    }
    if code == ERR_CONCURRENT_TRANSACTIONS || is_retriable(code) {
        return ok(true);
    }
    return err(msg);
}

// Flushes and commits: every record and offset of the transaction becomes
// visible to read_committed consumers. On error, abort (or, when fenced,
// close the producer).
pub fn commit_transaction(p: Producer, deadline: until) -> result[bool, str] {
    return end_transaction(p, true, deadline);
}

// Sends what is queued, then aborts: read_committed consumers never see
// the transaction's records.
pub fn abort_transaction(p: Producer, deadline: until) -> result[bool, str] {
    return end_transaction(p, false, deadline);
}

fn end_transaction(p: Producer, commit: bool, deadline: until) -> result[bool, str] {
    let cr = txn_check(p);
    guard let _c = cr else let e = err_of(cr) { return err(e); }
    if !p.txn.in_txn {
        return err("kafka: no transaction is open");
    }
    add_txn_partitions(p);
    if p.txn.fatal != "" {
        fail_queued(p, p.txn.fatal);
        return err(p.txn.fatal);
    }
    p.txn.committing = true;
    let fr = flush(p, deadline);
    p.txn.committing = false;
    if commit {
        guard let _f = fr else let e = err_of(fr) {
            txn_abortable(p, e, false);
            return err("kafka: the transaction cannot commit (abort it): " + strip_prefix(e));
        }
        if p.txn.abortable != "" {
            return err("kafka: the transaction cannot commit (abort it): " + strip_prefix(p.txn.abortable));
        }
    }
    if p.txn.fatal != "" {
        return err(p.txn.fatal);
    }
    if len(p.txn.added) == 0 && !p.txn.offsets_added {
        // nothing reached the coordinator: nothing to end
        reset_txn(p);
        return ok(true);
    }
    let last = "";
    while true {
        let body = new_buf();
        enc_end_txn(body, p.opts.transactional_id, p.pid, p.epoch, commit);
        let rr = txn_or_any_call(p, API_END_TXN, body, deadline);
        if let r = rr {
            let code = dec_error_only(r, "EndTxn") ?? -1;
            if code == 0 {
                reset_txn(p);
                return ok(true);
            }
            let what = "commit";
            if !commit {
                what = "abort";
            }
            last = kerr(what + " the transaction", code, "");
            let tr = txn_code(p, code, last);
            guard let _t = tr else let e = err_of(tr) { return err(e); }
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

fn reset_txn(p: Producer) {
    mutex_lock(p.lock);
    p.txn.in_txn = false;
    p.txn.parts = {};
    p.txn.added = {};
    p.txn.abortable = "";
    p.txn.offsets_added = false;
    mutex_unlock(p.lock);
}
