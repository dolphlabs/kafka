// The cluster client and admin calls against in-process clusters
// (tests/kfake: franz-go's kfake): metadata, topics, partitions, offsets,
// configs and groups; SASL PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512; TLS
// with a private CA; bad seeds. tests/run.sh starts the clusters and
// passes their addresses in KAFKA_PLAIN, KAFKA_SASL, KAFKA_TLS and KAFKA_CA.

import "kafka";
import "proc";
import "strings";

fn soon() -> until {
    return kafka.after_ms(10000);
}

fn env(name: str) -> str {
    let v = proc.getenv(name) ?? "";
    if v == "" {
        panic("set " + name + " (tests/run.sh starts the clusters)");
    }
    return v;
}

// errors with the clusters' ports replaced, so the output is stable
fn clean(e: str) -> str {
    let out = e;
    for name in ["KAFKA_PLAIN", "KAFKA_SASL", "KAFKA_TLS"] {
        for a in strings.split(proc.getenv(name) ?? "", ",") {
            if a != "" {
                out = strings.replace(out, a, name);
            }
        }
    }
    return out;
}

fn cfg_for(addrs: str) -> kafka.Config {
    return kafka.config(strings.split(addrs, ","));
}

fn show(what: str, r: result[bool, str]) {
    if let v = r {
        println(what + ": ok " + to_str(v));
    } else let e = err_of(r) {
        println(what + ": " + e);
    }
}

fn topic_line(t: kafka.TopicInfo) -> str {
    let leaders = 0;
    for p in t.partitions {
        if p.leader >= 0 {
            leaders += 1;
        }
    }
    return t.name + " partitions=" + to_str(len(t.partitions)) + " with_leaders=" + to_str(leaders);
}

fn has_topic(ts: [kafka.TopicInfo], name: str) -> bool {
    for t in ts {
        if t.name == name {
            return true;
        }
    }
    return false;
}

fn plain_cluster() {
    let clr = kafka.connect(cfg_for(env("KAFKA_PLAIN")), soon());
    guard let cl = clr else let e = err_of(clr) { println("connect: " + e); return; }
    println("connect: cluster=" + kafka.cluster_id(cl) + " brokers=" + to_str(len(kafka.brokers(cl))) +
            " controller_known=" + to_str(kafka.controller(cl) >= 0));
    let lr = kafka.list_topics(cl, soon());
    guard let ts = lr else let e = err_of(lr) { println("list_topics: " + e); return; }
    for t in ts {
        if t.name == "seeded" {
            println("list_topics: " + topic_line(t));
        }
    }
    show("create orders", kafka.create_topic(cl, "orders", 6, 1, soon()));
    show("create orders again", kafka.create_topic(cl, "orders", 6, 1, soon()));
    let dr = kafka.describe_topic(cl, "orders", soon());
    if let t = dr {
        println("describe orders: " + topic_line(t));
    } else let e = err_of(dr) {
        println("describe orders: " + e);
    }
    show("grow orders to 8", kafka.create_partitions(cl, "orders", 8, soon()));
    let dr2 = kafka.describe_topic(cl, "orders", soon());
    if let t = dr2 {
        println("describe orders: " + topic_line(t));
    } else let e = err_of(dr2) {
        println("describe orders: " + e);
    }
    let er = kafka.list_offset(cl, "orders", 0, kafka.OFFSET_EARLIEST, soon());
    let ler = kafka.list_offset(cl, "orders", 7, kafka.OFFSET_LATEST, soon());
    println("offsets orders/0 earliest=" + to_str(er ?? -99) + " orders/7 latest=" + to_str(ler ?? -99));
    let mr = kafka.list_offset(cl, "orders", 99, kafka.OFFSET_LATEST, soon());
    println("offsets orders/99: " + err_of_or(mr));
    let nr = kafka.describe_topic(cl, "no-such-topic", soon());
    println("describe no-such-topic: " + err_of_or2(nr));
    show("set retention.ms", kafka.alter_configs(cl, kafka.RESOURCE_TOPIC, "orders",
                                                  [kafka.ConfigOp { name: "retention.ms", op: kafka.CONFIG_SET,
                                                                    value: some("3600000") }], soon()));
    let cr = kafka.describe_configs(cl, kafka.RESOURCE_TOPIC, "orders", soon());
    if let cs = cr {
        for c in cs {
            if c.name == "retention.ms" {
                println("retention.ms=" + (c.value ?? "null") + " source=" + to_str(c.source));
            }
        }
    } else let e = err_of(cr) {
        println("describe_configs: " + e);
    }
    let gr = kafka.list_groups(cl, soon());
    println("groups: " + to_str(len(gr ?? [])));
    show("delete orders", kafka.delete_topics(cl, ["orders"], soon()));
    let lr2 = kafka.list_topics(cl, soon());
    println("orders listed after delete: " + to_str(has_topic(lr2 ?? [], "orders")));
    kafka.close(cl);
    show("after close", kafka.create_topic(cl, "x", 1, 1, soon()));
}

fn err_of_or(r: result[int, str]) -> str {
    if let v = r {
        return "ok " + to_str(v);
    }
    return err_of(r);
}

fn err_of_or2(r: result[kafka.TopicInfo, str]) -> str {
    if let v = r {
        return "ok " + v.name;
    }
    return err_of(r);
}

fn try_sasl(what: str, addrs: str, s: kafka.Sasl, allow_plain: bool) {
    let cfg = cfg_for(addrs);
    cfg.sasl = s;
    cfg.sasl_plain_without_tls = allow_plain;
    let clr = kafka.connect(cfg, soon());
    if let cl = clr {
        println(what + ": connected, " + to_str(len(kafka.brokers(cl))) + " broker");
        kafka.close(cl);
    } else let e = err_of(clr) {
        println(what + ": " + clean(e));
    }
}

fn sasl_cluster() {
    let a = env("KAFKA_SASL");
    try_sasl("no sasl", a, kafka.no_sasl(), false);
    try_sasl("plain without tls", a, kafka.plain("plain-user", "plain-pass"), false);
    try_sasl("plain allowed", a, kafka.plain("plain-user", "plain-pass"), true);
    try_sasl("plain wrong password", a, kafka.plain("plain-user", "nope"), true);
    try_sasl("scram-sha-256", a, kafka.scram_sha256("scram256", "pass-256"), false);
    try_sasl("scram-sha-256 wrong password", a, kafka.scram_sha256("scram256", "nope"), false);
    try_sasl("scram-sha-512", a, kafka.scram_sha512("scram512", "pass-512"), false);
    try_sasl("scram-sha-512 unknown user", a, kafka.scram_sha512("nobody", "x"), false);
}

fn tls_cluster() {
    let a = env("KAFKA_TLS");
    let cfg = cfg_for(a);
    cfg.tls = true;
    cfg.ca_path = env("KAFKA_CA");
    cfg.sasl = kafka.scram_sha512("app", "app-secret");
    let clr = kafka.connect(cfg, soon());
    if let cl = clr {
        show("tls+scram create", kafka.create_topic(cl, "secure", 1, 1, soon()));
        kafka.close(cl);
    } else let e = err_of(clr) {
        println("tls+scram: " + e);
    }
    let bad = cfg_for(a);
    bad.tls = true;
    bad.sasl = kafka.scram_sha512("app", "app-secret");
    let br = kafka.connect(bad, soon());
    if let cl = br {
        println("tls without the CA: connected (wrong)");
        kafka.close(cl);
    } else let e = err_of(br) {
        println("tls without the CA refused: " + to_str(strings.contains(e, "kafka: tls")));
    }
}

fn seeds() {
    let good = strings.split(env("KAFKA_PLAIN"), ",")[0];
    let cfg = kafka.config(["127.0.0.1:1", good]);
    let clr = kafka.connect(cfg, soon());
    if let cl = clr {
        println("dead seed then a live one: connected to " + to_str(len(kafka.brokers(cl))) + " brokers");
        kafka.close(cl);
    } else let e = err_of(clr) {
        println("dead seed then a live one: " + e);
    }
    let dead = kafka.config(["127.0.0.1:1"]);
    println("only dead seeds: " + err_of_or3(kafka.connect(dead, soon())));
    println("no port: " + err_of_or3(kafka.connect(kafka.config(["localhost"]), soon())));
}

fn err_of_or3(r: result[kafka.Client, str]) -> str {
    if let cl = r {
        kafka.close(cl);
        return "connected";
    }
    return err_of(r);
}

plain_cluster();
sasl_cluster();
tls_cluster();
seeds();
