// Admin calls: topics, partitions, records, groups and configs.
//
//     kafka.create_topic(cl, "orders", 12, 3, deadline);    // result[bool, str]: false if it existed
//     let ts = kafka.list_topics(cl, deadline);            // result[[TopicInfo], str]
//     kafka.list_offset(cl, "orders", 0, kafka.OFFSET_LATEST, deadline)
//
// Topic changes go to the controller (any broker forwards them in KRaft
// clusters); group calls go to the group's coordinator.

// Every topic in the cluster (internal ones included), sorted by name.
pub fn list_topics(cl: Client, deadline: until) -> result[[TopicInfo], str] {
    let body = new_buf();
    enc_metadata(body, none, false);
    let rr = call_any(cl, API_METADATA, body, deadline);
    guard let r = rr else let e = err_of(rr) { return err(e); }
    let mr = dec_metadata(r);
    guard let m = mr else let e = err_of(mr) { return err(e); }
    apply_metadata(cl, m, true);
    let out: [TopicInfo] = [];
    for t in m.topics {
        push(out, index_partitions(t));
    }
    sort_topics(out);
    return ok(out);
}

fn sort_topics(ts: [TopicInfo]) {
    // insertion sort: topic lists are short
    for i in 1..len(ts) {
        let x = ts[i];
        let j = i - 1;
        while j >= 0 && ts[j].name > x.name {
            ts[j + 1] = ts[j];
            j -= 1;
        }
        ts[j + 1] = x;
    }
}

// The topic's partitions and leaders, fresh from a broker.
pub fn describe_topic(cl: Client, topic: str, deadline: until) -> result[TopicInfo, str] {
    let ur = use_topics(cl, [topic], deadline);
    guard let _u = ur else let e = err_of(ur) { return err(e); }
    let rr = refresh_metadata(cl, deadline);
    guard let _r = rr else let e = err_of(rr) { return err(e); }
    let ti = topic_info(cl, topic);
    guard let t = ti else { return err("kafka: no metadata for topic " + topic); }
    if t.error_code != 0 {
        return err(kerr("topic " + topic, t.error_code, ""));
    }
    return ok(t);
}

// The controller's connection, refreshing metadata once if it is unknown.
fn controller_call(cl: Client, key: int, body: Buf, deadline: until) -> result[Reader, str] {
    if cl.controller < 0 {
        let rr = refresh_metadata(cl, deadline);
        guard let _r = rr else let e = err_of(rr) { return err(e); }
    }
    return call_node(cl, cl.controller, "ctl", key, body, deadline);
}

// Creates topics; each result is the topic's error code (0, or for
// example TOPIC_ALREADY_EXISTS) and the broker's message.
pub fn create_topics(cl: Client, topics: [NewTopic], deadline: until) -> result[[NamedError], str] {
    let body = new_buf();
    enc_create_topics(body, topics, 30000, false);
    let rr = controller_call(cl, API_CREATE_TOPICS, body, deadline);
    guard let r = rr else let e = err_of(rr) { return err(e); }
    let cr = dec_create_topics(r);
    guard let created = cr else let e = err_of(cr) { return err(e); }
    let out: [NamedError] = [];
    for t in created {
        push(out, NamedError { name: t.name, error_code: t.error_code, message: t.error_message });
    }
    return ok(out);
}

// Creates one topic: ok(true) when created, ok(false) when it already
// existed. partitions or replication_factor -1 take the broker defaults.
pub fn create_topic(cl: Client, name: str, partitions: int, replication_factor: int, deadline: until)
                    -> result[bool, str] {
    let t = NewTopic { name: name, partitions: partitions, replication_factor: replication_factor, assignments: [],
                       configs: [] };
    let rr = create_topics(cl, [t], deadline);
    guard let res = rr else let e = err_of(rr) { return err(e); }
    for x in res {
        if x.error_code == ERR_TOPIC_ALREADY_EXISTS {
            return ok(false);
        }
        if x.error_code != 0 {
            return err(kerr("create topic " + name, x.error_code, x.message));
        }
    }
    use_topics(cl, [name], deadline);
    return ok(true);
}

pub fn delete_topics(cl: Client, names: [str], deadline: until) -> result[bool, str] {
    let body = new_buf();
    enc_delete_topics(body, names, 30000);
    let rr = controller_call(cl, API_DELETE_TOPICS, body, deadline);
    guard let r = rr else let e = err_of(rr) { return err(e); }
    let dr = dec_delete_topics(r);
    guard let res = dr else let e = err_of(dr) { return err(e); }
    for x in res {
        if x.error_code != 0 {
            return err(kerr("delete topic " + x.name, x.error_code, x.message));
        }
    }
    mutex_lock(cl.lock);
    for n in names {
        if has(cl.topics, n) {
            del(cl.topics, n);
        }
        if has(cl.interest, n) {
            del(cl.interest, n);
        }
    }
    mutex_unlock(cl.lock);
    return ok(true);
}

// Grows a topic to count partitions.
pub fn create_partitions(cl: Client, topic: str, count: int, deadline: until) -> result[bool, str] {
    let body = new_buf();
    enc_create_partitions(body, [NewPartitions { topic: topic, count: count, assignments: [] }], 30000, false);
    let rr = controller_call(cl, API_CREATE_PARTITIONS, body, deadline);
    guard let r = rr else let e = err_of(rr) { return err(e); }
    let dr = dec_create_partitions(r);
    guard let res = dr else let e = err_of(dr) { return err(e); }
    for x in res {
        if x.error_code != 0 {
            return err(kerr("create partitions for " + x.name, x.error_code, x.message));
        }
    }
    metadata_stale(cl);
    return ok(true);
}

// The leader of topic/partition, refreshing metadata when unknown.
fn leader_or_refresh(cl: Client, topic: str, partition: int, deadline: until) -> result[int, str] {
    let ur = use_topics(cl, [topic], deadline);
    guard let _u = ur else let e = err_of(ur) { return err(e); }
    let leader = leader_of(cl, topic, partition);
    if leader < 0 {
        let rr = refresh_metadata(cl, deadline);
        guard let _r = rr else let e = err_of(rr) { return err(e); }
        leader = leader_of(cl, topic, partition);
    }
    if leader < 0 {
        let ti = topic_info(cl, topic);
        if let t = ti {
            if t.error_code != 0 {
                return err(kerr("topic " + topic, t.error_code, ""));
            }
            if partition >= len(t.partitions) {
                return err("kafka: topic " + topic + " has no partition " + to_str(partition));
            }
        }
        return err("kafka: " + topic + "/" + to_str(partition) + " has no leader");
    }
    return ok(leader);
}

// The offset for timestamp (ms) in a partition: the first offset whose
// record is at or after it, or with OFFSET_EARLIEST / OFFSET_LATEST the
// log's start or end (the next offset to be written).
pub fn list_offset(cl: Client, topic: str, partition: int, timestamp: int, deadline: until) -> result[int, str] {
    let last = "";
    for attempt in 0..3 {
        let lr = leader_or_refresh(cl, topic, partition, deadline);
        guard let leader = lr else let e = err_of(lr) { return err(e); }
        let body = new_buf();
        enc_list_offsets(body, READ_UNCOMMITTED,
                         [OffsetQuery { topic: topic, partition: partition, leader_epoch: -1, timestamp: timestamp }]);
        let rr = call_node(cl, leader, "ctl", API_LIST_OFFSETS, body, deadline);
        guard let r = rr else let e = err_of(rr) { return err(e); }
        let dr = dec_list_offsets(r);
        guard let res = dr else let e = err_of(dr) { return err(e); }
        for x in res {
            if x.error_code == 0 {
                return ok(x.offset);
            }
            last = kerr("list offsets " + topic + "/" + to_str(partition), x.error_code, "");
            if !is_retriable(x.error_code) {
                return err(last);
            }
        }
        refresh_metadata(cl, deadline);
    }
    return err(last);
}

// Deletes the records before offset in a partition; returns the new log
// start offset.
pub fn delete_records(cl: Client, topic: str, partition: int, offset: int, deadline: until) -> result[int, str] {
    let lr = leader_or_refresh(cl, topic, partition, deadline);
    guard let leader = lr else let e = err_of(lr) { return err(e); }
    let body = new_buf();
    enc_delete_records(body, [PartOffset { topic: topic, partition: partition, offset: offset }], 30000);
    let rr = call_node(cl, leader, "ctl", API_DELETE_RECORDS, body, deadline);
    guard let r = rr else let e = err_of(rr) { return err(e); }
    let dr = dec_delete_records(r);
    guard let res = dr else let e = err_of(dr) { return err(e); }
    for x in res {
        if x.error_code != 0 {
            return err(kerr("delete records " + topic + "/" + to_str(partition), x.error_code, ""));
        }
        return ok(x.low_watermark);
    }
    return err("kafka: an empty DeleteRecords response");
}

// ---- groups -----------------------------------------------------------------------

// Every consumer group, from every broker.
pub fn list_groups(cl: Client, deadline: until) -> result[[ListedGroup], str] {
    let out: [ListedGroup] = [];
    for b in brokers(cl) {
        let body = new_buf();
        enc_list_groups(body);
        let rr = call_node(cl, b.node_id, "ctl", API_LIST_GROUPS, body, deadline);
        guard let r = rr else let e = err_of(rr) { return err(e); }
        let lr = dec_list_groups(r);
        guard let l = lr else let e = err_of(lr) { return err(e); }
        if l.error_code != 0 {
            return err(kerr("list groups on broker " + to_str(b.node_id), l.error_code, ""));
        }
        for g in l.groups {
            push(out, g);
        }
    }
    return ok(out);
}

// A call to a group's coordinator, finding it again when it moved.
fn group_call(cl: Client, group: str, key: int, body: Buf, deadline: until) -> result[Reader, str] {
    let nr = coordinator(cl, group, COORDINATOR_GROUP, deadline);
    guard let node = nr else let e = err_of(nr) { return err(e); }
    let rr = call_node(cl, node, "ctl", key, body, deadline);
    if let r = rr {
        return ok(r);
    } else let e = err_of(rr) {
        forget_coordinator(cl, group, COORDINATOR_GROUP);
        return err(e);
    }
}

pub fn describe_group(cl: Client, group: str, deadline: until) -> result[GroupDescription, str] {
    for attempt in 0..3 {
        let body = new_buf();
        enc_describe_groups(body, [group]);
        let rr = group_call(cl, group, API_DESCRIBE_GROUPS, body, deadline);
        guard let r = rr else let e = err_of(rr) { return err(e); }
        let dr = dec_describe_groups(r);
        guard let gs = dr else let e = err_of(dr) { return err(e); }
        if len(gs) != 1 {
            return err("kafka: DescribeGroups answered for " + to_str(len(gs)) + " groups");
        }
        let g = gs[0];
        if g.error_code == ERR_NOT_COORDINATOR || g.error_code == ERR_COORDINATOR_NOT_AVAILABLE {
            forget_coordinator(cl, group, COORDINATOR_GROUP);
            continue;
        }
        if g.error_code != 0 {
            return err(kerr("describe group " + group, g.error_code, ""));
        }
        return ok(g);
    }
    return err("kafka: describe group " + group + ": its coordinator kept moving");
}

// Deletes an empty group and its committed offsets.
pub fn delete_group(cl: Client, group: str, deadline: until) -> result[bool, str] {
    let body = new_buf();
    enc_delete_groups(body, [group]);
    let rr = group_call(cl, group, API_DELETE_GROUPS, body, deadline);
    guard let r = rr else let e = err_of(rr) { return err(e); }
    let dr = dec_delete_groups(r);
    guard let res = dr else let e = err_of(dr) { return err(e); }
    for x in res {
        if x.error_code != 0 {
            return err(kerr("delete group " + group, x.error_code, ""));
        }
    }
    return ok(true);
}

// The offsets a group has committed (every partition).
pub fn committed_offsets(cl: Client, group: str, deadline: until) -> result[[CommittedOffset], str] {
    let body = new_buf();
    enc_offset_fetch(body, group, none, false);
    let rr = group_call(cl, group, API_OFFSET_FETCH, body, deadline);
    guard let r = rr else let e = err_of(rr) { return err(e); }
    let dr = dec_offset_fetch(r);
    guard let res = dr else let e = err_of(dr) { return err(e); }
    if res.error_code != 0 {
        return err(kerr("committed offsets of " + group, res.error_code, ""));
    }
    return ok(res.offsets);
}

// ---- configs ----------------------------------------------------------------------

// A topic's (RESOURCE_TOPIC) or broker's (RESOURCE_BROKER, name the node
// id) configs.
pub fn describe_configs(cl: Client, kind: int, name: str, deadline: until) -> result[[ConfigValue], str] {
    let body = new_buf();
    enc_describe_configs(body, [ConfigResource { kind: kind, name: name, keys: none }]);
    let rr = config_call(cl, kind, name, API_DESCRIBE_CONFIGS, body, deadline);
    guard let r = rr else let e = err_of(rr) { return err(e); }
    let dr = dec_describe_configs(r);
    guard let res = dr else let e = err_of(dr) { return err(e); }
    for d in res {
        if d.error_code != 0 {
            return err(kerr("describe configs of " + name, d.error_code, d.error_message));
        }
        return ok(d.configs);
    }
    return err("kafka: an empty DescribeConfigs response");
}

// Sets (CONFIG_SET), removes (CONFIG_DELETE), appends to or subtracts from
// configs, leaving the others as they are.
pub fn alter_configs(cl: Client, kind: int, name: str, ops: [ConfigOp], deadline: until) -> result[bool, str] {
    let body = new_buf();
    enc_alter_configs(body, [ConfigAlteration { kind: kind, name: name, ops: ops }], false);
    let rr = config_call(cl, kind, name, API_INCREMENTAL_ALTER_CONFIGS, body, deadline);
    guard let r = rr else let e = err_of(rr) { return err(e); }
    let dr = dec_alter_configs(r);
    guard let res = dr else let e = err_of(dr) { return err(e); }
    for x in res {
        if x.error_code != 0 {
            return err(kerr("alter configs of " + name, x.error_code, x.message));
        }
    }
    return ok(true);
}

// A broker's configs live on that broker; topic configs on any.
fn config_call(cl: Client, kind: int, name: str, key: int, body: Buf, deadline: until) -> result[Reader, str] {
    if kind == RESOURCE_BROKER {
        let nr = to_int(name);
        guard let node = nr else { return err("kafka: a broker config resource is named by its node id"); }
        return call_node(cl, node, "ctl", key, body, deadline);
    }
    return call_any(cl, key, body, deadline);
}
