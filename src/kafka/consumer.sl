// The consumer: partitions it is given (assign) or that a consumer group
// gives it (subscribe, group.sl), fetched in the background, handed out
// in batches by poll.
//
//     let co = kafka.consumer_options("billing");
//     co.reset = kafka.Reset.Earliest;
//     let cr = kafka.subscribe(cl, ["orders"], co, deadline);
//     guard let c = cr else let e = err_of(cr) { panic(e); }
//     while running {
//         let pr = kafka.poll(c, kafka.after_ms(1000));     // result[[Message], str]; [] at the deadline
//         guard let msgs = pr else let e = err_of(pr) { panic(e); }
//         for m in msgs { handle(m); }
//         kafka.commit(c, deadline);                        // or co.auto_commit (the default)
//     }
//     kafka.close_consumer(c, deadline);                     // commits, leaves the group
//
// A fetch task sends one Fetch per broker for the partitions it leads,
// over the "fetch" connection, and queues what comes back per partition.
// A partition is fetched again once poll has taken its queue, and nothing
// is fetched while buffer_bytes are queued, so a slow consumer stops
// fetching instead of growing.

import "strings";

pub enum Reset { Earliest, Latest, None }

pub gc struct ConsumerOptions {
    // "" for a consumer without a group (assign only, no commits)
    group: str,
    // static membership: a restart with the same id keeps the assignment
    // without a rebalance (within session_timeout_ms)
    instance_id: str,
    // where to start when there is no committed offset (or it is gone)
    reset: Reset,
    // READ_UNCOMMITTED or READ_COMMITTED (skips aborted transactions)
    isolation: int,
    auto_commit: bool,
    auto_commit_interval_ms: int,
    session_timeout_ms: int,
    rebalance_timeout_ms: int,
    heartbeat_interval_ms: int,
    // in order of preference: "range", "roundrobin", "sticky"
    assignors: [str],
    fetch_max_wait_ms: int,
    fetch_min_bytes: int,
    fetch_max_bytes: int,
    partition_max_bytes: int,
    max_poll_records: int,
    // fetched bytes queued before fetching pauses
    buffer_bytes: int,
    check_crcs: bool,
}

pub fn consumer_options(group: str) -> ConsumerOptions {
    return ConsumerOptions { group: group, instance_id: "", reset: Reset.Latest, isolation: READ_UNCOMMITTED,
                             auto_commit: true, auto_commit_interval_ms: 5000, session_timeout_ms: 45000,
                             rebalance_timeout_ms: 60000, heartbeat_interval_ms: 3000,
                             assignors: ["range", "roundrobin", "sticky"], fetch_max_wait_ms: 500,
                             fetch_min_bytes: 1, fetch_max_bytes: 52428800, partition_max_bytes: 1048576,
                             max_poll_records: 500, buffer_bytes: 67108864, check_crcs: true };
}

gc struct PartState {
    // unique per consumer: a reassigned partition is a new state
    id: int,
    topic: str,
    partition: int,
    // the next offset to fetch (-1: not resolved yet)
    position: int,
    // the offset after the last record poll returned (-1: none yet)
    consumed: int,
    committed: int,
    // the next resolution skips the committed offset (it was out of range,
    // or seek asked for the start or end)
    need_reset: bool,
    // OFFSET_EARLIEST or OFFSET_LATEST from seek; 0 for the reset policy
    reset_to: int,
    fetching: bool,
    // bumped by seek, so a fetch answered after it is dropped
    seq: int,
    queue: [Message],
    qhead: int,
    qbytes: int,
    paused: bool,
    high_watermark: int,
}

pub gc struct Consumer {
    cl: Client,
    opts: ConsumerOptions,
    lock: mutex,
    subscribed: [str],
    parts: map[str]PartState,
    order: [PartState],
    buffered: int,
    ready: chan[bool],
    wake: chan[bool],
    // the consumer cannot go on
    fatal: str,
    // reported once by the next poll
    pending_err: str,
    closing: bool,
    closed: bool,
    rr: int,
    next_id: int,
    // this consumer's connections: "fetch#<uid>", "group:<id>#<uid>"
    uid: int,
    // brokers with a fetch in flight: one at a time per broker, so a
    // long-polling fetch never holds up one with data behind it on the
    // same connection
    fetching_nodes: map[int]bool,
    // the group (group.sl)
    member_id: str,
    generation: int,
    leader_id: str,
    protocol: str,
    rejoin: bool,
    assigned_counts: map[str]int,
    last_commit_ns: int,
    group_done: chan[bool],
    fetch_done: chan[bool],
}

fn new_consumer(cl: Client, o: ConsumerOptions) -> result[Consumer, str] {
    if o.max_poll_records < 1 || o.buffer_bytes < o.partition_max_bytes || o.fetch_max_wait_ms < 0 {
        return err("kafka: consumer options out of range (max_poll_records >= 1, buffer_bytes >= partition_max_bytes)");
    }
    if o.isolation != READ_UNCOMMITTED && o.isolation != READ_COMMITTED {
        return err("kafka: isolation is READ_UNCOMMITTED or READ_COMMITTED");
    }
    return ok(Consumer { cl: cl, opts: o, lock: make_mutex(), subscribed: [], parts: {}, order: [], buffered: 0,
                         ready: make_chan(1), wake: make_chan(1), fatal: "", pending_err: "", closing: false,
                         closed: false, rr: 0, next_id: 0, uid: next_consumer_uid(cl), fetching_nodes: {}, member_id: "", generation: -1, leader_id: "", protocol: "",
                         rejoin: false, assigned_counts: {}, last_commit_ns: now_ns(), group_done: make_chan(1),
                         fetch_done: make_chan(1) });
}

// A consumer of exactly these partitions, without group membership. With
// opts.group set it starts from (and can commit) that group's offsets.
pub fn assign(cl: Client, tps: [TopicPartition], o: ConsumerOptions, deadline: until) -> result[Consumer, str] {
    let cr = new_consumer(cl, o);
    guard let c = cr else let e = err_of(cr) { return err(e); }
    let topics: [str] = [];
    for x in tps {
        push(topics, x.topic);
    }
    let ur = use_topics(cl, topics, deadline);
    guard let _u = ur else let e = err_of(ur) { return err(e); }
    set_assignment(c, tps);
    spawn fetch_loop(c);
    if o.group != "" && o.auto_commit {
        spawn commit_loop(c);
    } else {
        chan_send(c.group_done, true);
    }
    return ok(c);
}

// A broker serves a connection's requests one at a time, so two
// consumers in one group must not share one: the first's JoinGroup waits
// for the second, whose JoinGroup would wait behind it.
fn next_consumer_uid(cl: Client) -> int {
    mutex_lock(cl.lock);
    cl.uids += 1;
    let v = cl.uids;
    mutex_unlock(cl.lock);
    return v;
}

fn part_key(topic: str, partition: int) -> str {
    return topic + "/" + itoa(partition);
}

// Replaces the assignment (an eager rebalance: everything is new).
fn set_assignment(c: Consumer, tps: [TopicPartition]) {
    mutex_lock(c.lock);
    let parts: map[str]PartState = {};
    let order: [PartState] = [];
    for x in tps {
        let key = part_key(x.topic, x.partition);
        if has(parts, key) {
            continue;
        }
        c.next_id += 1;
        let ps = PartState { id: c.next_id, topic: x.topic, partition: x.partition, position: -1, consumed: -1, committed: -1,
                             need_reset: false, reset_to: 0, fetching: false, seq: 0, queue: [], qhead: 0, qbytes: 0, paused: false,
                             high_watermark: -1 };
        parts[key] = ps;
        push(order, ps);
    }
    c.parts = parts;
    c.order = order;
    c.buffered = 0;
    mutex_unlock(c.lock);
    kick(c.wake);
}

fn kick(ch: chan[bool]) {
    select {
        case chan_send(ch, true) { }
        default { }
    }
}

// The partitions this consumer has now.
pub fn assignment(c: Consumer) -> [TopicPartition] {
    mutex_lock(c.lock);
    let out: [TopicPartition] = [];
    for ps in c.order {
        push(out, TopicPartition { topic: ps.topic, partition: ps.partition });
    }
    mutex_unlock(c.lock);
    return out;
}

// ---- polling ------------------------------------------------------------------------

// Up to max_poll_records fetched records, waiting until deadline for the
// first; [] when none came. Records of one partition come in offset order.
pub fn poll(c: Consumer, deadline: until) -> result[[Message], str] {
    while true {
        if c.fatal != "" {
            return err(c.fatal);
        }
        if c.closing || c.closed {
            return err("kafka: the consumer is closed");
        }
        if c.pending_err != "" {
            let e = c.pending_err;
            c.pending_err = "";
            return err(e);
        }
        let out = take(c);
        if len(out) > 0 {
            kick(c.wake);
            return ok(out);
        }
        if until_hit(deadline) {
            return ok(out);
        }
        let w = add_waiter(c.cl.clock, deadline, until_never());
        select {
            case let _r = chan_recv(c.ready) { }
            case let _t = chan_recv(w.wake) { }
        }
        w.done = true;
    }
    return ok([]);
}

fn take(c: Consumer) -> [Message] {
    let out: [Message] = [];
    mutex_lock(c.lock);
    let n = len(c.order);
    let left = c.opts.max_poll_records;
    for k in 0..n {
        if left == 0 {
            break;
        }
        let ps = c.order[(c.rr + k) % n];
        if ps.paused {
            continue;
        }
        while ps.qhead < len(ps.queue) && left > 0 {
            let m = ps.queue[ps.qhead];
            ps.qhead += 1;
            push(out, m);
            ps.consumed = m.offset + 1;
            left -= 1;
        }
        if ps.qhead >= len(ps.queue) && len(ps.queue) > 0 {
            c.buffered -= ps.qbytes;
            ps.queue = [];
            ps.qhead = 0;
            ps.qbytes = 0;
        }
    }
    if n > 0 {
        c.rr = (c.rr + 1) % n;
    }
    mutex_unlock(c.lock);
    return out;
}

// Starts topic/partition at offset (the next poll returns it).
pub fn seek(c: Consumer, topic: str, partition: int, offset: int) -> result[bool, str] {
    mutex_lock(c.lock);
    let key = part_key(topic, partition);
    if !has(c.parts, key) {
        mutex_unlock(c.lock);
        return err("kafka: " + key + " is not assigned to this consumer");
    }
    let ps = c.parts[key];
    ps.seq += 1;
    ps.position = offset;
    ps.need_reset = false;
    ps.reset_to = 0;
    if offset == OFFSET_EARLIEST || offset == OFFSET_LATEST {
        ps.position = -1;
        ps.need_reset = true;
        ps.reset_to = offset;
    }
    ps.consumed = -1;
    c.buffered -= ps.qbytes;
    ps.queue = [];
    ps.qhead = 0;
    ps.qbytes = 0;
    mutex_unlock(c.lock);
    kick(c.wake);
    return ok(true);
}

// Stops (pause) and restarts (resume) handing out a partition's records,
// keeping its position.
pub fn pause(c: Consumer, topic: str, partition: int) {
    set_paused(c, topic, partition, true);
}

pub fn resume(c: Consumer, topic: str, partition: int) {
    set_paused(c, topic, partition, false);
}

fn set_paused(c: Consumer, topic: str, partition: int, v: bool) {
    mutex_lock(c.lock);
    let key = part_key(topic, partition);
    if has(c.parts, key) {
        c.parts[key].paused = v;
    }
    mutex_unlock(c.lock);
    kick(c.wake);
}

// The offset the next poll starts topic/partition from (-1 if unknown).
pub fn position(c: Consumer, topic: str, partition: int) -> int {
    mutex_lock(c.lock);
    let key = part_key(topic, partition);
    let v = -1;
    if has(c.parts, key) {
        let ps = c.parts[key];
        v = ps.position;
        if ps.consumed >= 0 {
            v = ps.consumed;
        } else if ps.qhead < len(ps.queue) {
            v = ps.queue[ps.qhead].offset;
        }
    }
    mutex_unlock(c.lock);
    return v;
}

// The partition's end (high watermark) from the last fetch (-1 unknown).
pub fn high_watermark(c: Consumer, topic: str, partition: int) -> int {
    mutex_lock(c.lock);
    let key = part_key(topic, partition);
    let v = -1;
    if has(c.parts, key) {
        v = c.parts[key].high_watermark;
    }
    mutex_unlock(c.lock);
    return v;
}

// ---- fetching -----------------------------------------------------------------------

gc struct FetchPlan {
    node: int,
    parts: [PartState],
    seqs: [int],
    offsets: [int],
}

fn fetch_loop(c: Consumer) {
    while !c.closed && !c.closing {
        resolve_positions(c);
        let plans = plan_fetches(c);
        for node, plan in plans {
            spawn fetch_one(c, plan);
        }
        let w = add_waiter(c.cl.clock, after_ms(100), until_never());
        select {
            case let _k = chan_recv(c.wake) { }
            case let _t = chan_recv(w.wake) { }
        }
        w.done = true;
    }
    kick(c.fetch_done);
}

// A broker's partitions are fetched together, once everything the last
// fetch from it returned has been polled: a fetch that holds a partition
// with data comes back at once, so one caught-up partition never makes
// the others wait out fetch_max_wait_ms.
fn plan_fetches(c: Consumer) -> map[int]FetchPlan {
    let plans: map[int]FetchPlan = {};
    mutex_lock(c.lock);
    if c.buffered >= c.opts.buffer_bytes {
        mutex_unlock(c.lock);
        return plans;
    }
    let busy: map[int]bool = {};
    let leaders: map[str]int = {};
    for ps in c.order {
        let leader = leader_of(c.cl, ps.topic, ps.partition);
        leaders[part_key(ps.topic, ps.partition)] = leader;
        if leader < 0 {
            metadata_stale(c.cl);
            continue;
        }
        if has(c.fetching_nodes, leader) || (ps.qhead < len(ps.queue) && !ps.paused) {
            busy[leader] = true;
        }
    }
    for ps in c.order {
        let leader = leaders[part_key(ps.topic, ps.partition)];
        if leader < 0 || has(busy, leader) || ps.paused || ps.fetching || ps.position < 0 ||
           ps.qhead < len(ps.queue) {
            continue;
        }
        if !has(plans, leader) {
            plans[leader] = FetchPlan { node: leader, parts: [], seqs: [], offsets: [] };
        }
        ps.fetching = true;
        push(plans[leader].parts, ps);
        push(plans[leader].seqs, ps.seq);
        push(plans[leader].offsets, ps.position);
    }
    for node, _p in plans {
        c.fetching_nodes[node] = true;
    }
    mutex_unlock(c.lock);
    return plans;
}

fn fetch_one(c: Consumer, plan: FetchPlan) {
    let topics: [FetchTopic] = [];
    let at: map[str]int = {};
    for i in 0..len(plan.parts) {
        let ps = plan.parts[i];
        if !has(at, ps.topic) {
            at[ps.topic] = len(topics);
            push(topics, FetchTopic { topic: ps.topic, parts: [] });
        }
        push(topics[at[ps.topic]].parts, FetchPart { partition: ps.partition, leader_epoch: -1, offset: plan.offsets[i],
                                                     last_fetched_epoch: -1, max_bytes: c.opts.partition_max_bytes });
    }
    let f = FetchReq { max_wait_ms: c.opts.fetch_max_wait_ms, min_bytes: c.opts.fetch_min_bytes,
                       max_bytes: c.opts.fetch_max_bytes, isolation: c.opts.isolation, session_id: 0, session_epoch: -1,
                       topics: topics, forgotten: [], rack: c.cl.cfg.rack };
    let got = false;
    let cr = conn_to(c.cl, plan.node, "fetch#" + to_str(c.uid), after_ms(c.cl.cfg.request_timeout_ms));
    if let conn = cr {
        let v = conn_version(conn, API_FETCH);
        let body = new_buf();
        enc_fetch_v(body, v, f);
        let rr = call_timeout(conn, API_FETCH, body, after_ms(c.opts.fetch_max_wait_ms + c.cl.cfg.request_timeout_ms),
                              c.opts.fetch_max_wait_ms + c.cl.cfg.request_timeout_ms);
        if let r = rr {
            let dr = dec_fetch_v(r, v);
            if let res = dr {
                got = handle_fetch(c, plan, res);
            } else let e = err_of(dr) {
                c.pending_err = e;
            }
        } else let e = err_of(rr) {
            metadata_stale(c.cl);
            sleep_ms(c.cl.cfg.retry_backoff_ms);
        }
    } else let e = err_of(cr) {
        metadata_stale(c.cl);
        sleep_ms(c.cl.cfg.retry_backoff_ms);
    }
    mutex_lock(c.lock);
    for ps in plan.parts {
        ps.fetching = false;
    }
    del(c.fetching_nodes, plan.node);
    mutex_unlock(c.lock);
    if got {
        kick(c.ready);
    }
    kick(c.wake);
}

// Queues what a fetch returned; true when any record was queued.
fn handle_fetch(c: Consumer, plan: FetchPlan, res: FetchResp) -> bool {
    if res.error_code != 0 {
        c.pending_err = kerr("fetch from broker " + to_str(plan.node), res.error_code, "");
        return false;
    }
    let by: map[str]FetchedPart = {};
    for p in res.parts {
        by[part_key(p.topic, p.partition)] = p;
    }
    let any = false;
    for i in 0..len(plan.parts) {
        let ps = plan.parts[i];
        let key = part_key(ps.topic, ps.partition);
        if !has(by, key) {
            continue;
        }
        let fp = by[key];
        let code = fp.error_code;
        if code == ERR_OFFSET_OUT_OF_RANGE {
            mutex_lock(c.lock);
            if ps.seq == plan.seqs[i] {
                if c.opts.reset == Reset.None {
                    c.pending_err = kerr("fetch " + key + " at offset " + to_str(plan.offsets[i]), code,
                                         "the offset is gone and reset is None");
                } else {
                    ps.position = -1;
                    ps.need_reset = true;
                }
            }
            mutex_unlock(c.lock);
            continue;
        }
        if code != 0 {
            if is_retriable(code) {
                metadata_stale(c.cl);
            } else {
                c.pending_err = kerr("fetch " + key, code, "");
            }
            continue;
        }
        let dr = decode_part(c, ps, fp, res.frame, plan.offsets[i]);
        guard let d = dr else let e = err_of(dr) {
            c.pending_err = "kafka: fetch " + key + ": " + strip_prefix(e);
            continue;
        }
        mutex_lock(c.lock);
        // dropped when the partition was sought, revoked or reassigned meanwhile
        let current = has(c.parts, key) && c.parts[key].id == ps.id;
        if current && ps.seq == plan.seqs[i] && ps.position == plan.offsets[i] {
            ps.high_watermark = fp.high_watermark;
            if d.next > ps.position {
                ps.position = d.next;
            }
            if len(d.msgs) > 0 {
                ps.queue = d.msgs;
                ps.qhead = 0;
                ps.qbytes = d.size;
                c.buffered += d.size;
                any = true;
            }
        }
        mutex_unlock(c.lock);
    }
    return any;
}

gc struct Decoded {
    msgs: [Message],
    next: int,
    size: int,
}

// A partition's batches as messages from offset on: control batches
// skipped, and with read_committed, the records of aborted transactions.
fn decode_part(c: Consumer, ps: PartState, fp: FetchedPart, frame: bytes, offset: int) -> result[Decoded, str] {
    let d = Decoded { msgs: [], next: offset, size: 0 };
    let r = reader(frame, fp.records_from, fp.records_to);
    let aborted: [Aborted] = sort_aborted(fp.aborted ?? []);
    let ai = 0;
    let active: map[int]bool = {};
    let committed_only = c.opts.isolation == READ_COMMITTED;
    while true {
        let br = read_batch(r, c.cl.crc, c.opts.check_crcs);
        guard let got = br else let e = err_of(br) { return err(e); }
        guard let b = got else { break; }
        while ai < len(aborted) && aborted[ai].first_offset <= b.last_offset() {
            active[aborted[ai].producer_id] = true;
            ai += 1;
        }
        let skip = false;
        if b.is_control() {
            skip = true;
            if committed_only && has(active, b.producer_id) {
                let tr = control_type(c.cl.crc, b);
                if (tr ?? -1) == CONTROL_ABORT {
                    del(active, b.producer_id);
                }
            }
        } else if committed_only && b.is_transactional() && has(active, b.producer_id) {
            skip = true;
        }
        if !skip {
            let mr = batch_messages(c.cl.crc, b, ps.topic, ps.partition, c.opts.fetch_max_bytes);
            guard let ms = mr else let e = err_of(mr) { return err(e); }
            for m in ms {
                if m.offset >= offset {
                    push(d.msgs, m);
                    d.size += 64 + len(m.key ?? b"") + len(m.value ?? b"");
                }
            }
        }
        if b.next_offset() > d.next {
            d.next = b.next_offset();
        }
    }
    return ok(d);
}

fn sort_aborted(xs: [Aborted]) -> [Aborted] {
    let out: [Aborted] = [];
    for x in xs {
        push(out, x);
    }
    for i in 1..len(out) {
        let x = out[i];
        let j = i - 1;
        while j >= 0 && out[j].first_offset > x.first_offset {
            out[j + 1] = out[j];
            j -= 1;
        }
        out[j + 1] = x;
    }
    return out;
}

// ---- positions --------------------------------------------------------------------

// Positions for partitions that have none: the group's committed offset,
// else the reset policy (earliest or latest).
fn resolve_positions(c: Consumer) {
    let need: [PartState] = [];
    mutex_lock(c.lock);
    for ps in c.order {
        if ps.position < 0 {
            push(need, ps);
        }
    }
    mutex_unlock(c.lock);
    if len(need) == 0 {
        return;
    }
    let deadline = after_ms(c.cl.cfg.request_timeout_ms);
    let to_reset: [PartState] = [];
    let ask: [PartState] = [];
    for ps in need {
        if ps.need_reset || c.opts.group == "" {
            push(to_reset, ps);
        } else {
            push(ask, ps);
        }
    }
    if len(ask) > 0 {
        let cr = fetch_committed(c, ask, deadline);
        guard let found = cr else let e = err_of(cr) {
            if !strings.contains(e, "UNSTABLE_OFFSET_COMMIT") && !strings.contains(e, "timed out") {
                c.pending_err = e;
            }
            sleep_ms(c.cl.cfg.retry_backoff_ms);
            return;
        }
        for ps in ask {
            let key = part_key(ps.topic, ps.partition);
            if has(found, key) && found[key] >= 0 {
                mutex_lock(c.lock);
                if ps.position < 0 {
                    ps.position = found[key];
                    ps.committed = found[key];
                }
                mutex_unlock(c.lock);
            } else {
                push(to_reset, ps);
            }
        }
    }
    for ps in to_reset {
        let ts = OFFSET_LATEST;
        if ps.reset_to != 0 {
            ts = ps.reset_to;
        } else if c.opts.reset == Reset.Earliest {
            ts = OFFSET_EARLIEST;
        } else if c.opts.reset == Reset.None {
            c.pending_err = "kafka: " + part_key(ps.topic, ps.partition) +
                            " has no committed offset and reset is None";
            continue;
        }
        let lr = list_offset(c.cl, ps.topic, ps.partition, ts, deadline);
        guard let off = lr else let e = err_of(lr) {
            metadata_stale(c.cl);
            continue;
        }
        mutex_lock(c.lock);
        if ps.position < 0 {
            ps.position = off;
            ps.need_reset = false;
            ps.reset_to = 0;
        }
        mutex_unlock(c.lock);
    }
}

// The group's committed offsets for parts, by "topic/partition".
fn fetch_committed(c: Consumer, parts: [PartState], deadline: until) -> result[map[str]int, str] {
    let tps: [TopicParts] = [];
    let at: map[str]int = {};
    for ps in parts {
        if !has(at, ps.topic) {
            at[ps.topic] = len(tps);
            push(tps, TopicParts { topic: ps.topic, partitions: [] });
        }
        push(tps[at[ps.topic]].partitions, ps.partition);
    }
    let body = new_buf();
    enc_offset_fetch(body, c.opts.group, some(tps), c.opts.isolation == READ_COMMITTED);
    let rr = coordinator_call(c, API_OFFSET_FETCH, body, deadline);
    guard let r = rr else let e = err_of(rr) { return err(e); }
    let dr = dec_offset_fetch(r);
    guard let res = dr else let e = err_of(dr) { return err(e); }
    if res.error_code != 0 {
        if res.error_code == ERR_NOT_COORDINATOR || res.error_code == ERR_COORDINATOR_NOT_AVAILABLE {
            forget_coordinator(c.cl, c.opts.group, COORDINATOR_GROUP);
        }
        return err(kerr("committed offsets of " + c.opts.group, res.error_code, ""));
    }
    let out: map[str]int = {};
    for o in res.offsets {
        if o.error_code != 0 {
            return err(kerr("committed offset of " + part_key(o.topic, o.partition), o.error_code, ""));
        }
        out[part_key(o.topic, o.partition)] = o.offset;
    }
    return ok(out);
}

// A call to the group's coordinator over the group's connection.
fn coordinator_call(c: Consumer, key: int, body: Buf, deadline: until) -> result[Reader, str] {
    return coordinator_call_timeout(c, key, body, deadline, c.cl.cfg.request_timeout_ms);
}

fn coordinator_call_timeout(c: Consumer, key: int, body: Buf, deadline: until, timeout_ms: int)
                            -> result[Reader, str] {
    let nr = coordinator(c.cl, c.opts.group, COORDINATOR_GROUP, deadline);
    guard let node = nr else let e = err_of(nr) { return err(e); }
    let rr = call_node_timeout(c.cl, node, "group:" + c.opts.group + "#" + to_str(c.uid), key, body, deadline,
                               timeout_ms);
    if let r = rr {
        return ok(r);
    } else let e = err_of(rr) {
        forget_coordinator(c.cl, c.opts.group, COORDINATOR_GROUP);
        return err(e);
    }
}

// ---- committing --------------------------------------------------------------------

// Commits, for every partition, the offset after the last record poll
// returned.
pub fn commit(c: Consumer, deadline: until) -> result[bool, str] {
    let cs: [OffsetToCommit] = [];
    let states: [PartState] = [];
    mutex_lock(c.lock);
    for ps in c.order {
        if ps.consumed >= 0 && ps.consumed != ps.committed {
            push(cs, OffsetToCommit { topic: ps.topic, partition: ps.partition, offset: ps.consumed, leader_epoch: -1,
                                      metadata: none });
            push(states, ps);
        }
    }
    mutex_unlock(c.lock);
    if len(cs) == 0 {
        return ok(true);
    }
    let cr = commit_offsets(c, cs, deadline);
    guard let _c = cr else let e = err_of(cr) { return err(e); }
    mutex_lock(c.lock);
    for i in 0..len(states) {
        states[i].committed = cs[i].offset;
    }
    mutex_unlock(c.lock);
    return ok(true);
}

// Commits these offsets (each the next offset to consume) for the group.
pub fn commit_offsets(c: Consumer, cs: [OffsetToCommit], deadline: until) -> result[bool, str] {
    if c.opts.group == "" {
        return err("kafka: committing needs a group (ConsumerOptions.group)");
    }
    let last = "";
    while true {
        let generation = -1;
        let member = "";
        if len(c.subscribed) > 0 {
            if c.generation < 0 {
                return err("kafka: commit: the group is rebalancing; poll again and commit after");
            }
            generation = c.generation;
            member = c.member_id;
        }
        let body = new_buf();
        enc_offset_commit(body, c.opts.group, generation, member, instance_opt(c), cs);
        let rr = coordinator_call(c, API_OFFSET_COMMIT, body, deadline);
        if let r = rr {
            let dr = dec_part_errors(r, "OffsetCommit");
            guard let res = dr else let e = err_of(dr) { return err(e); }
            let worst = 0;
            let where = "";
            for x in res {
                if x.error_code != 0 {
                    worst = x.error_code;
                    where = part_key(x.topic, x.partition);
                }
            }
            if worst == 0 {
                c.last_commit_ns = now_ns();
                return ok(true);
            }
            last = kerr("commit " + where, worst, "");
            if worst == ERR_REBALANCE_IN_PROGRESS || worst == ERR_ILLEGAL_GENERATION || worst == ERR_UNKNOWN_MEMBER_ID ||
               worst == ERR_FENCED_INSTANCE_ID {
                c.rejoin = true;
                return err(last);
            }
            if worst == ERR_NOT_COORDINATOR || worst == ERR_COORDINATOR_NOT_AVAILABLE {
                forget_coordinator(c.cl, c.opts.group, COORDINATOR_GROUP);
            } else if !is_retriable(worst) {
                return err(last);
            }
        } else let e = err_of(rr) {
            last = e;
        }
        if until_hit(deadline) {
            return err(last);
        }
        sleep_ms(c.cl.cfg.retry_backoff_ms);
    }
    return err(last);
}

fn instance_opt(c: Consumer) -> opt[str] {
    if c.opts.instance_id == "" {
        return none;
    }
    return some(c.opts.instance_id);
}

// Auto-commit for an assigned (groupless-membership) consumer.
fn commit_loop(c: Consumer) {
    while !c.closing && !c.closed {
        sleep_ms(100);
        if (now_ns() - c.last_commit_ns) / MS >= c.opts.auto_commit_interval_ms {
            commit(c, after_ms(c.cl.cfg.request_timeout_ms));
            c.last_commit_ns = now_ns();
        }
    }
    kick(c.group_done);
}

// Commits (with auto_commit), leaves the group, and stops fetching.
pub fn close_consumer(c: Consumer, deadline: until) -> result[bool, str] {
    if c.closed {
        return ok(true);
    }
    c.closing = true;
    kick(c.wake);
    let w = add_waiter(c.cl.clock, deadline, until_never());
    select {
        case let _g = chan_recv(c.group_done) { }
        case let _t = chan_recv(w.wake) { }
    }
    w.done = true;
    let cr = ok(true);
    if c.opts.auto_commit && c.opts.group != "" && len(c.subscribed) == 0 {
        cr = commit(c, deadline);
    }
    c.closed = true;
    kick(c.wake);
    close_purpose(c.cl, "fetch#" + to_str(c.uid));
    close_purpose(c.cl, "group:" + c.opts.group + "#" + to_str(c.uid));
    return cr;
}
