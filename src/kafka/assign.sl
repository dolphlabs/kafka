// The consumer protocol inside JoinGroup and SyncGroup, and the
// assignors the group leader runs.
//
// A member's subscription (JoinGroup metadata) is a version, its topics,
// user data and, from v1, the partitions it owns; v2 adds a generation
// and v3 a rack, which this package reads and does not write. An
// assignment (SyncGroup) is a version, topics with their partitions, and
// user data. Both use classic (non-flexible) encodings, so Java, franz-go
// and librdkafka members of the same group read them.
//
// Assignors: "range" (contiguous ranges per topic, as Java's
// RangeAssignor), "roundrobin" (all partitions dealt in turn), and
// "sticky" (every member keeps what it owned while that stays balanced,
// then the rest is dealt to the members with the fewest).

pub gc struct TopicPartition {
    topic: str,
    partition: int,
}

pub fn tp(topic: str, partition: int) -> TopicPartition {
    return TopicPartition { topic: topic, partition: partition };
}

pub gc struct Subscription {
    member_id: str,
    instance_id: opt[str],
    topics: [str],
    owned: [TopicPartition],
}

pub fn encode_subscription(topics: [str], owned: [TopicPartition]) -> bytes {
    let w = new_buf();
    wr_i16(w, 1);
    wr_len(w, len(topics));
    for t in topics {
        wr_str(w, t);
    }
    wr_i32(w, -1);
    wr_tps(w, owned);
    return w.finish();
}

fn wr_tps(w: Buf, tps: [TopicPartition]) {
    let groups: [[TopicPartition]] = [];
    let at: map[str]int = {};
    for x in tps {
        if !has(at, x.topic) {
            at[x.topic] = len(groups);
            let fresh: [TopicPartition] = [];
            push(groups, fresh);
        }
        push(groups[at[x.topic]], x);
    }
    wr_len(w, len(groups));
    for g in groups {
        wr_str(w, g[0].topic);
        wr_len(w, len(g));
        for x in g {
            wr_i32(w, x.partition);
        }
    }
}

fn rd_tps(r: Reader) -> [TopicPartition] {
    let out: [TopicPartition] = [];
    let nt = rd_len(r);
    for k in 0..nt {
        let topic = rd_str(r);
        let np = rd_len(r);
        for j in 0..np {
            push(out, TopicPartition { topic: topic, partition: rd_i32(r) });
        }
    }
    return out;
}

pub fn decode_subscription(member_id: str, instance_id: opt[str], b: bytes) -> result[Subscription, str] {
    let r = reader(b, 0, len(b));
    let version = rd_i16(r);
    let s = Subscription { member_id: member_id, instance_id: instance_id, topics: [], owned: [] };
    let nt = rd_len(r);
    for k in 0..nt {
        push(s.topics, rd_str(r));
    }
    rd_nbytes(r);
    if version >= 1 && r.left() > 0 {
        s.owned = rd_tps(r);
    }
    if !r.good() {
        return err("kafka: member " + member_id + " sent a malformed subscription: " + r.failed);
    }
    return ok(s);
}

pub fn encode_assignment(tps: [TopicPartition]) -> bytes {
    let w = new_buf();
    wr_i16(w, 1);
    wr_tps(w, tps);
    wr_i32(w, -1);
    return w.finish();
}

pub fn decode_assignment(b: bytes) -> result[[TopicPartition], str] {
    if len(b) == 0 {
        let none_yet: [TopicPartition] = [];
        return ok(none_yet);
    }
    let r = reader(b, 0, len(b));
    rd_i16(r);
    let tps = rd_tps(r);
    if !r.good() {
        return err("kafka: a malformed assignment from the group leader: " + r.failed);
    }
    return ok(tps);
}

// ---- assignors ----------------------------------------------------------------------

fn sorted_strs(xs: [str]) -> [str] {
    let out: [str] = [];
    for x in xs {
        push(out, x);
    }
    for i in 1..len(out) {
        let x = out[i];
        let j = i - 1;
        while j >= 0 && out[j] > x {
            out[j + 1] = out[j];
            j -= 1;
        }
        out[j + 1] = x;
    }
    return out;
}

fn subscribes(s: Subscription, topic: str) -> bool {
    for t in s.topics {
        if t == topic {
            return true;
        }
    }
    return false;
}

// Members sorted by member id (group instance id first when set, as Java
// sorts static members, so a restarted member keeps its place).
fn sorted_members(subs: [Subscription]) -> [Subscription] {
    let out: [Subscription] = [];
    for s in subs {
        push(out, s);
    }
    for i in 1..len(out) {
        let x = out[i];
        let j = i - 1;
        while j >= 0 && member_key(out[j]) > member_key(x) {
            out[j + 1] = out[j];
            j -= 1;
        }
        out[j + 1] = x;
    }
    return out;
}

fn member_key(s: Subscription) -> str {
    if let i = s.instance_id {
        return "0" + i;
    }
    return "1" + s.member_id;
}

fn all_topics(subs: [Subscription]) -> [str] {
    let seen: map[str]bool = {};
    let out: [str] = [];
    for s in subs {
        for t in s.topics {
            if !has(seen, t) {
                seen[t] = true;
                push(out, t);
            }
        }
    }
    return sorted_strs(out);
}

fn empty_plan(subs: [Subscription]) -> map[str][TopicPartition] {
    let plan: map[str][TopicPartition] = {};
    for s in subs {
        let none_yet: [TopicPartition] = [];
        plan[s.member_id] = none_yet;
    }
    return plan;
}

// counts: partitions per topic (topics missing from it are skipped).
pub fn run_assignor(protocol: str, subs: [Subscription], counts: map[str]int) -> result[map[str][TopicPartition], str] {
    if protocol == "range" {
        return ok(assign_range(subs, counts));
    }
    if protocol == "roundrobin" {
        return ok(assign_roundrobin(subs, counts));
    }
    if protocol == "sticky" {
        return ok(assign_sticky(subs, counts));
    }
    return err("kafka: the group chose assignor " + protocol + ", which this client does not have");
}

fn assign_range(subs: [Subscription], counts: map[str]int) -> map[str][TopicPartition] {
    let plan = empty_plan(subs);
    let members = sorted_members(subs);
    for t in all_topics(subs) {
        if !has(counts, t) {
            continue;
        }
        let ms: [Subscription] = [];
        for m in members {
            if subscribes(m, t) {
                push(ms, m);
            }
        }
        let n = counts[t];
        let each = n / len(ms);
        let extra = n % len(ms);
        let next = 0;
        for i in 0..len(ms) {
            let k = each;
            if i < extra {
                k += 1;
            }
            for j in 0..k {
                push(plan[ms[i].member_id], TopicPartition { topic: t, partition: next });
                next += 1;
            }
        }
    }
    return plan;
}

fn assign_roundrobin(subs: [Subscription], counts: map[str]int) -> map[str][TopicPartition] {
    let plan = empty_plan(subs);
    let members = sorted_members(subs);
    let at = 0;
    for t in all_topics(subs) {
        if !has(counts, t) {
            continue;
        }
        for p in 0..counts[t] {
            // the next member, in turn, that subscribes to t
            for k in 0..len(members) {
                let m = members[(at + k) % len(members)];
                if subscribes(m, t) {
                    push(plan[m.member_id], TopicPartition { topic: t, partition: p });
                    at = (at + k + 1) % len(members);
                    break;
                }
            }
        }
    }
    return plan;
}

// Keeps owned partitions up to each member's fair share, then gives the
// rest to the members with the fewest that can take them.
fn assign_sticky(subs: [Subscription], counts: map[str]int) -> map[str][TopicPartition] {
    let plan = empty_plan(subs);
    let members = sorted_members(subs);
    let total = 0;
    let all: [TopicPartition] = [];
    for t in all_topics(subs) {
        if !has(counts, t) {
            continue;
        }
        for p in 0..counts[t] {
            push(all, TopicPartition { topic: t, partition: p });
        }
    }
    total = len(all);
    if len(members) == 0 {
        return plan;
    }
    let quota = (total + len(members) - 1) / len(members);
    let taken: map[str]bool = {};
    for m in members {
        for o in m.owned {
            let key = o.topic + "/" + itoa(o.partition);
            if has(taken, key) || !has(counts, o.topic) || o.partition >= counts[o.topic] || !subscribes(m, o.topic) {
                continue;
            }
            if len(plan[m.member_id]) >= quota {
                break;
            }
            taken[key] = true;
            push(plan[m.member_id], o);
        }
    }
    for x in all {
        let key = x.topic + "/" + itoa(x.partition);
        if has(taken, key) {
            continue;
        }
        let best = -1;
        for i in 0..len(members) {
            let m = members[i];
            if !subscribes(m, x.topic) {
                continue;
            }
            if best < 0 || len(plan[m.member_id]) < len(plan[members[best].member_id]) {
                best = i;
            }
        }
        if best >= 0 {
            taken[key] = true;
            push(plan[members[best].member_id], x);
        }
    }
    return plan;
}
