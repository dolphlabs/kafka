// Consumer groups (the classic protocol, eager rebalancing): a task per
// consumer joins the group, runs the assignor when it is the leader,
// takes its partitions from SyncGroup, heartbeats, commits on a timer
// (auto_commit), and on a rebalance commits what poll returned and gives
// every partition up before joining again.
//
// The group's requests go to its coordinator over a connection of their
// own ("group:<id>"), since a JoinGroup can wait for the whole rebalance.

pub fn subscribe(cl: Client, topics: [str], o: ConsumerOptions, deadline: until) -> result[Consumer, str] {
    if o.group == "" {
        return err("kafka: subscribe needs a group (ConsumerOptions.group); use assign without one");
    }
    if len(topics) == 0 {
        return err("kafka: subscribe to at least one topic");
    }
    if len(o.assignors) == 0 {
        return err("kafka: at least one assignor (range, roundrobin, sticky)");
    }
    for a in o.assignors {
        if a != "range" && a != "roundrobin" && a != "sticky" {
            return err("kafka: unknown assignor " + a);
        }
    }
    if o.heartbeat_interval_ms <= 0 || o.heartbeat_interval_ms * 3 > o.session_timeout_ms {
        return err("kafka: heartbeat_interval_ms must be positive and at most a third of session_timeout_ms");
    }
    let cr = new_consumer(cl, o);
    guard let c = cr else let e = err_of(cr) { return err(e); }
    c.subscribed = topics;
    let ur = use_topics(cl, topics, deadline);
    guard let _u = ur else let e = err_of(ur) { return err(e); }
    let nr = coordinator(cl, o.group, COORDINATOR_GROUP, deadline);
    guard let _n = nr else let e = err_of(nr) { return err(e); }
    spawn group_loop(c);
    spawn fetch_loop(c);
    return ok(c);
}

// The group's member id and generation (-1 while rebalancing).
pub fn member_id(c: Consumer) -> str {
    return c.member_id;
}

pub fn generation(c: Consumer) -> int {
    return c.generation;
}

fn group_loop(c: Consumer) {
    while !c.closing && !c.closed {
        let jr = join_and_sync(c);
        if let joined = jr {
            if !joined {
                sleep_ms(c.cl.cfg.retry_backoff_ms);
                continue;
            }
        } else let e = err_of(jr) {
            c.fatal = e;
            break;
        }
        stay(c);
    }
    if c.generation >= 0 {
        if c.opts.auto_commit {
            commit(c, after_ms(c.cl.cfg.request_timeout_ms));
        }
        if c.opts.instance_id == "" {
            leave(c);
        }
    }
    c.generation = -1;
    kick(c.group_done);
}

// ok(true): joined and assigned; ok(false): try again; err: give up.
fn join_and_sync(c: Consumer) -> result[bool, str] {
    let jr = join_once(c);
    guard let j = jr else let e = err_of(jr) { return err(e); }
    guard let resp = j else { return ok(false); }
    c.member_id = resp.member_id;
    c.protocol = resp.protocol;
    c.leader_id = resp.leader;
    let assignments: [MemberAssignment] = [];
    if resp.leader == resp.member_id {
        let pr = plan_group(c, resp);
        guard let plan = pr else let e = err_of(pr) { return err(e); }
        assignments = plan;
    }
    let body = new_buf();
    enc_sync_group(body, c.opts.group, resp.generation, c.member_id, instance_opt(c), assignments);
    let rr = coordinator_call_timeout(c, API_SYNC_GROUP, body, after_ms(c.opts.rebalance_timeout_ms + 5000),
                                      c.opts.rebalance_timeout_ms + 5000);
    guard let r = rr else { return ok(false); }
    let sr = dec_sync_group(r);
    guard let s = sr else let e = err_of(sr) { return err(e); }
    if s.error_code != 0 {
        return group_error(c, s.error_code, "SyncGroup");
    }
    let ar = decode_assignment(s.assignment);
    guard let mine = ar else let e = err_of(ar) { return err(e); }
    let wanted: [TopicPartition] = [];
    for x in mine {
        for t in c.subscribed {
            if t == x.topic {
                push(wanted, x);
            }
        }
    }
    let topics: [str] = [];
    for x in wanted {
        push(topics, x.topic);
    }
    use_topics(c.cl, topics, after_ms(c.cl.cfg.request_timeout_ms));
    set_assignment(c, wanted);
    c.generation = resp.generation;
    c.rejoin = false;
    c.last_commit_ns = now_ns();
    return ok(true);
}

// JoinGroup until the broker gives this member a generation: none when
// the caller should back off and retry.
fn join_once(c: Consumer) -> result[opt[JoinResp], str] {
    for attempt in 0..5 {
        let owned = assignment(c);
        let protocols: [GroupProtocol] = [];
        for a in c.opts.assignors {
            push(protocols, GroupProtocol { name: a, metadata: encode_subscription(c.subscribed, owned) });
        }
        let body = new_buf();
        enc_join_group(body, JoinReq { group: c.opts.group, session_timeout_ms: c.opts.session_timeout_ms,
                                       rebalance_timeout_ms: c.opts.rebalance_timeout_ms, member_id: c.member_id,
                                       instance_id: instance_opt(c), protocol_type: "consumer",
                                       protocols: protocols });
        let rr = coordinator_call_timeout(c, API_JOIN_GROUP, body, after_ms(c.opts.rebalance_timeout_ms + 5000),
                                          c.opts.rebalance_timeout_ms + 5000);
        guard let r = rr else { return ok(none); }
        let jr = dec_join_group(r);
        guard let resp = jr else let e = err_of(jr) { return err(e); }
        let code = resp.error_code;
        if code == 0 {
            return ok(some(resp));
        }
        if code == ERR_MEMBER_ID_REQUIRED {
            // KIP-394: the broker names the id to join with
            c.member_id = resp.member_id;
            continue;
        }
        let gr = group_error(c, code, "JoinGroup");
        guard let _g = gr else let e = err_of(gr) { return err(e); }
        if code != ERR_REBALANCE_IN_PROGRESS && code != ERR_UNKNOWN_MEMBER_ID {
            return ok(none);
        }
    }
    return ok(none);
}

// What a group error means for the member: ok(false) to try again,
// err when the consumer cannot go on.
fn group_error(c: Consumer, code: int, what: str) -> result[bool, str] {
    if code == ERR_UNKNOWN_MEMBER_ID || code == ERR_ILLEGAL_GENERATION {
        if c.opts.instance_id == "" {
            c.member_id = "";
        }
        return ok(false);
    }
    if code == ERR_NOT_COORDINATOR || code == ERR_COORDINATOR_NOT_AVAILABLE {
        forget_coordinator(c.cl, c.opts.group, COORDINATOR_GROUP);
        return ok(false);
    }
    if code == ERR_REBALANCE_IN_PROGRESS || is_retriable(code) {
        return ok(false);
    }
    return err(kerr(what + " " + c.opts.group, code, ""));
}

// The leader's plan: every member's subscription through the chosen
// assignor, over the partition counts in the metadata.
fn plan_group(c: Consumer, resp: JoinResp) -> result[[MemberAssignment], str] {
    let subs: [Subscription] = [];
    let topics: [str] = [];
    let seen: map[str]bool = {};
    for m in resp.members {
        let sr = decode_subscription(m.member_id, m.instance_id, m.metadata);
        guard let s = sr else let e = err_of(sr) { return err(e); }
        push(subs, s);
        for t in s.topics {
            if !has(seen, t) {
                seen[t] = true;
                push(topics, t);
            }
        }
    }
    use_topics(c.cl, topics, after_ms(c.cl.cfg.request_timeout_ms));
    let counts: map[str]int = {};
    for t in topics {
        let n = partition_count(c.cl, t);
        if n > 0 {
            counts[t] = n;
        }
    }
    let ar = run_assignor(resp.protocol, subs, counts);
    guard let plan = ar else let e = err_of(ar) { return err(e); }
    c.assigned_counts = counts;
    let out: [MemberAssignment] = [];
    for s in subs {
        push(out, MemberAssignment { member_id: s.member_id, assignment: encode_assignment(plan[s.member_id]) });
    }
    return ok(out);
}

// Heartbeats and auto-commits until a rebalance, the leader seeing a
// partition count change, or close.
fn stay(c: Consumer) {
    let next_hb = now_ns() + c.opts.heartbeat_interval_ms * MS;
    while !c.closing && !c.closed && !c.rejoin {
        sleep_ms(50);
        if c.opts.auto_commit && (now_ns() - c.last_commit_ns) / MS >= c.opts.auto_commit_interval_ms {
            commit(c, after_ms(c.cl.cfg.request_timeout_ms));
            c.last_commit_ns = now_ns();
        }
        if c.leader_id == c.member_id && counts_changed(c) {
            // the leader rejoins: the others learn of it from their heartbeats
            c.rejoin = true;
            break;
        }
        if now_ns() < next_hb {
            continue;
        }
        next_hb = now_ns() + c.opts.heartbeat_interval_ms * MS;
        let body = new_buf();
        enc_heartbeat(body, c.opts.group, c.generation, c.member_id, instance_opt(c));
        let rr = coordinator_call(c, API_HEARTBEAT, body, after_ms(c.opts.heartbeat_interval_ms));
        guard let r = rr else { continue; }
        let code = dec_error_only(r, "Heartbeat") ?? 0;
        if code == 0 {
            continue;
        }
        if code == ERR_REBALANCE_IN_PROGRESS {
            break;
        }
        if code == ERR_FENCED_INSTANCE_ID {
            c.fatal = kerr("heartbeat " + c.opts.group, code, "another consumer has instance id " + c.opts.instance_id);
            break;
        }
        let gr = group_error(c, code, "Heartbeat");
        if let _g = gr {
            if code == ERR_UNKNOWN_MEMBER_ID || code == ERR_ILLEGAL_GENERATION {
                c.generation = -1;
                break;
            }
        } else let e = err_of(gr) {
            c.fatal = e;
            break;
        }
    }
    if c.closing || c.closed {
        return;
    }
    // eager: commit what poll returned, then give everything up
    if c.opts.auto_commit && c.generation >= 0 {
        commit(c, after_ms(c.cl.cfg.request_timeout_ms));
    }
    c.generation = -1;
    set_assignment(c, []);
}

fn counts_changed(c: Consumer) -> bool {
    for t, n in c.assigned_counts {
        let now = partition_count(c.cl, t);
        if now > 0 && now != n {
            return true;
        }
    }
    for t in c.subscribed {
        if !has(c.assigned_counts, t) && partition_count(c.cl, t) > 0 {
            return true;
        }
    }
    return false;
}

fn leave(c: Consumer) {
    let body = new_buf();
    enc_leave_group(body, c.opts.group, c.member_id, none);
    coordinator_call(c, API_LEAVE_GROUP, body, after_ms(5000));
}
