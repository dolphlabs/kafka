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
}

fn no_txn() -> TxnState {
    return TxnState { in_txn: false, committing: false, parts: {}, added: {}, abortable: "", fatal: "" };
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

// Called under p.lock when send puts a record in topic/partition.
fn note_txn_partition(p: Producer, topic: str, partition: int) {
    p.txn.parts[topic + "/" + to_str(partition)] = true;
}

// Called under p.lock by the sender: a transactional batch waits until
// the coordinator has its partition.
fn txn_partition_added(p: Producer, topic: str, partition: int) -> bool {
    return has(p.txn.added, topic + "/" + to_str(partition));
}

fn txn_fatal(p: Producer, why: str) {
    if p.txn.fatal == "" {
        p.txn.fatal = why;
    }
}

fn txn_abortable(p: Producer, why: str, fenced: bool) {
    if fenced {
        txn_fatal(p, why);
    }
    if p.txn.abortable == "" {
        p.txn.abortable = why;
    }
}
