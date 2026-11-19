// Protocol vectors: every request the package sends and every response
// it reads, encoded by franz-go's kmsg at the package's API versions.
// tests/protocol encodes the same requests and must match byte for byte,
// and decodes the responses and must print what expected.txt says, which
// is written here from the values below, not by decoding.
//
//	cd tests/protocol/gen && go run .
package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func sp(s string) *string { return &s }

type vec struct {
	name string
	data []byte
}

var reqs, resps []vec
var want strings.Builder

func req(name string, r kmsg.Request, version int16) {
	r.SetVersion(version)
	reqs = append(reqs, vec{name, r.AppendTo(nil)})
	fmt.Fprintf(&want, "req %s same\n", name)
}

func resp(name string, r kmsg.Response, version int16, lines ...string) {
	r.SetVersion(version)
	resps = append(resps, vec{name, r.AppendTo(nil)})
	fmt.Fprintf(&want, "resp %s\n", name)
	for _, l := range lines {
		want.WriteString("  " + l + "\n")
	}
}

func ns(s *string) string {
	if s == nil {
		return "null"
	}
	return *s
}

func requests() {
	{
		r := kmsg.NewPtrApiVersionsRequest()
		r.ClientSoftwareName, r.ClientSoftwareVersion = "slang-kafka", "0.1.0"
		req("api_versions", r, 3)
	}
	{
		r := kmsg.NewPtrMetadataRequest()
		for _, t := range []string{"a", "bb"} {
			mt := kmsg.NewMetadataRequestTopic()
			mt.Topic = sp(t)
			r.Topics = append(r.Topics, mt)
		}
		r.AllowAutoTopicCreation = false
		req("metadata_topics", r, 9)
		r2 := kmsg.NewPtrMetadataRequest()
		r2.Topics = nil
		r2.AllowAutoTopicCreation = true
		req("metadata_all", r2, 9)
	}
	{
		r := kmsg.NewPtrFindCoordinatorRequest()
		r.CoordinatorKey, r.CoordinatorType = "group-1", 0
		req("find_coordinator_group", r, 3)
		r2 := kmsg.NewPtrFindCoordinatorRequest()
		r2.CoordinatorKey, r2.CoordinatorType = "txn-1", 1
		req("find_coordinator_txn", r2, 3)
	}
	{
		r := kmsg.NewPtrSASLHandshakeRequest()
		r.Mechanism = "SCRAM-SHA-512"
		req("sasl_handshake", r, 1)
		a := kmsg.NewPtrSASLAuthenticateRequest()
		a.SASLAuthBytes = []byte("n,,n=user,r=abc")
		req("sasl_authenticate", a, 2)
	}
	{
		r := kmsg.NewPtrProduceRequest()
		r.TransactionID = sp("tx")
		r.Acks, r.TimeoutMillis = -1, 30000
		t1 := kmsg.NewProduceRequestTopic()
		t1.Topic = "t1"
		for _, p := range []int32{0, 3} {
			pp := kmsg.NewProduceRequestTopicPartition()
			pp.Partition = p
			pp.Records = []byte{1, 2, 3, byte(p)}
			t1.Partitions = append(t1.Partitions, pp)
		}
		t2 := kmsg.NewProduceRequestTopic()
		t2.Topic = "t2"
		pp := kmsg.NewProduceRequestTopicPartition()
		pp.Partition = 1
		pp.Records = []byte("batch")
		t2.Partitions = append(t2.Partitions, pp)
		r.Topics = []kmsg.ProduceRequestTopic{t1, t2}
		req("produce", r, 9)
		r2 := kmsg.NewPtrProduceRequest()
		r2.Acks, r2.TimeoutMillis = 1, 1500
		r2.Topics = []kmsg.ProduceRequestTopic{t2}
		req("produce_no_txn", r2, 9)
	}
	{
		r := kmsg.NewPtrFetchRequest()
		r.ReplicaID, r.MaxWaitMillis, r.MinBytes, r.MaxBytes, r.IsolationLevel = -1, 500, 1, 52428800, 1
		r.SessionID, r.SessionEpoch = 7, 3
		t := kmsg.NewFetchRequestTopic()
		t.Topic = "t1"
		for _, p := range []int32{0, 2} {
			fp := kmsg.NewFetchRequestTopicPartition()
			fp.Partition, fp.CurrentLeaderEpoch, fp.FetchOffset, fp.LastFetchedEpoch, fp.LogStartOffset, fp.PartitionMaxBytes =
				p, 5, 100+int64(p), -1, -1, 1048576
			t.Partitions = append(t.Partitions, fp)
		}
		r.Topics = []kmsg.FetchRequestTopic{t}
		f := kmsg.NewFetchRequestForgottenTopic()
		f.Topic, f.Partitions = "t9", []int32{1, 2}
		r.ForgottenTopics = []kmsg.FetchRequestForgottenTopic{f}
		r.Rack = "rack-a"
		req("fetch", r, 12)
	}
	{
		r := kmsg.NewPtrListOffsetsRequest()
		r.ReplicaID, r.IsolationLevel = -1, 1
		t1 := kmsg.NewListOffsetsRequestTopic()
		t1.Topic = "t1"
		for _, q := range [][3]int64{{0, 4, -1}, {1, -1, -2}} {
			p := kmsg.NewListOffsetsRequestTopicPartition()
			p.Partition, p.CurrentLeaderEpoch, p.Timestamp = int32(q[0]), int32(q[1]), q[2]
			t1.Partitions = append(t1.Partitions, p)
		}
		t2 := kmsg.NewListOffsetsRequestTopic()
		t2.Topic = "t2"
		p := kmsg.NewListOffsetsRequestTopicPartition()
		p.Partition, p.CurrentLeaderEpoch, p.Timestamp = 0, -1, 1700000000000
		t2.Partitions = append(t2.Partitions, p)
		r.Topics = []kmsg.ListOffsetsRequestTopic{t1, t2}
		req("list_offsets", r, 6)
	}
	{
		r := kmsg.NewPtrJoinGroupRequest()
		r.Group, r.SessionTimeoutMillis, r.RebalanceTimeoutMillis, r.MemberID = "g", 45000, 300000, ""
		r.InstanceID = sp("i-1")
		r.ProtocolType = "consumer"
		for _, n := range []string{"range", "roundrobin"} {
			p := kmsg.NewJoinGroupRequestProtocol()
			p.Name, p.Metadata = n, []byte("meta-"+n)
			r.Protocols = append(r.Protocols, p)
		}
		req("join_group", r, 6)
	}
	{
		r := kmsg.NewPtrSyncGroupRequest()
		r.Group, r.Generation, r.MemberID = "g", 3, "m"
		for _, m := range []string{"m", "n"} {
			a := kmsg.NewSyncGroupRequestGroupAssignment()
			a.MemberID, a.MemberAssignment = m, []byte("assign-"+m)
			r.GroupAssignment = append(r.GroupAssignment, a)
		}
		req("sync_group", r, 4)
	}
	{
		r := kmsg.NewPtrHeartbeatRequest()
		r.Group, r.Generation, r.MemberID, r.InstanceID = "g", 3, "m", sp("i")
		req("heartbeat", r, 4)
	}
	{
		r := kmsg.NewPtrLeaveGroupRequest()
		r.Group = "g"
		m := kmsg.NewLeaveGroupRequestMember()
		m.MemberID = "m"
		r.Members = []kmsg.LeaveGroupRequestMember{m}
		req("leave_group", r, 4)
	}
	commits := func() []kmsg.OffsetCommitRequestTopic {
		t1 := kmsg.NewOffsetCommitRequestTopic()
		t1.Topic = "t1"
		for _, c := range []struct {
			p    int32
			o    int64
			e    int32
			meta *string
		}{{0, 100, 2, nil}, {1, 200, -1, sp("x")}} {
			p := kmsg.NewOffsetCommitRequestTopicPartition()
			p.Partition, p.Offset, p.LeaderEpoch, p.Metadata = c.p, c.o, c.e, c.meta
			t1.Partitions = append(t1.Partitions, p)
		}
		t2 := kmsg.NewOffsetCommitRequestTopic()
		t2.Topic = "t2"
		p := kmsg.NewOffsetCommitRequestTopicPartition()
		p.Partition, p.Offset, p.LeaderEpoch = 0, 5, 1
		t2.Partitions = append(t2.Partitions, p)
		return []kmsg.OffsetCommitRequestTopic{t1, t2}
	}
	{
		r := kmsg.NewPtrOffsetCommitRequest()
		r.Group, r.Generation, r.MemberID = "g", 3, "m"
		r.Topics = commits()
		req("offset_commit", r, 8)
	}
	{
		r := kmsg.NewPtrOffsetFetchRequest()
		r.Group = "g"
		t := kmsg.NewOffsetFetchRequestTopic()
		t.Topic, t.Partitions = "t1", []int32{0, 1}
		r.Topics = []kmsg.OffsetFetchRequestTopic{t}
		r.RequireStable = true
		req("offset_fetch", r, 7)
		r2 := kmsg.NewPtrOffsetFetchRequest()
		r2.Group = "g"
		r2.Topics = nil
		req("offset_fetch_all", r2, 7)
	}
	{
		r := kmsg.NewPtrDescribeGroupsRequest()
		r.Groups = []string{"g", "h"}
		req("describe_groups", r, 5)
		req("list_groups", kmsg.NewPtrListGroupsRequest(), 3)
		d := kmsg.NewPtrDeleteGroupsRequest()
		d.Groups = []string{"g"}
		req("delete_groups", d, 2)
	}
	{
		r := kmsg.NewPtrCreateTopicsRequest()
		t := kmsg.NewCreateTopicsRequestTopic()
		t.Topic, t.NumPartitions, t.ReplicationFactor = "t", 3, 2
		for _, c := range []struct {
			n string
			v *string
		}{{"cleanup.policy", sp("compact")}, {"retention.ms", nil}} {
			cc := kmsg.NewCreateTopicsRequestTopicConfig()
			cc.Name, cc.Value = c.n, c.v
			t.Configs = append(t.Configs, cc)
		}
		u := kmsg.NewCreateTopicsRequestTopic()
		u.Topic, u.NumPartitions, u.ReplicationFactor = "u", -1, -1
		for i, bs := range [][]int32{{1, 2}, {2, 3}} {
			a := kmsg.NewCreateTopicsRequestTopicReplicaAssignment()
			a.Partition, a.Replicas = int32(i), bs
			u.ReplicaAssignment = append(u.ReplicaAssignment, a)
		}
		r.Topics = []kmsg.CreateTopicsRequestTopic{t, u}
		r.TimeoutMillis = 30000
		req("create_topics", r, 5)
	}
	{
		r := kmsg.NewPtrDeleteTopicsRequest()
		r.TopicNames = []string{"t", "u"}
		r.TimeoutMillis = 30000
		req("delete_topics", r, 4)
	}
	{
		r := kmsg.NewPtrDeleteRecordsRequest()
		t := kmsg.NewDeleteRecordsRequestTopic()
		t.Topic = "t"
		for _, x := range [][2]int64{{0, 100}, {1, 200}} {
			p := kmsg.NewDeleteRecordsRequestTopicPartition()
			p.Partition, p.Offset = int32(x[0]), x[1]
			t.Partitions = append(t.Partitions, p)
		}
		u := kmsg.NewDeleteRecordsRequestTopic()
		u.Topic = "u"
		p := kmsg.NewDeleteRecordsRequestTopicPartition()
		p.Partition, p.Offset = 0, 5
		u.Partitions = append(u.Partitions, p)
		r.Topics = []kmsg.DeleteRecordsRequestTopic{t, u}
		r.TimeoutMillis = 30000
		req("delete_records", r, 2)
	}
	{
		r := kmsg.NewPtrCreatePartitionsRequest()
		t := kmsg.NewCreatePartitionsRequestTopic()
		t.Topic, t.Count = "t", 6
		u := kmsg.NewCreatePartitionsRequestTopic()
		u.Topic, u.Count = "u", 4
		for _, bs := range [][]int32{{1, 2}, {3, 1}} {
			a := kmsg.NewCreatePartitionsRequestTopicAssignment()
			a.Replicas = bs
			u.Assignment = append(u.Assignment, a)
		}
		r.Topics = []kmsg.CreatePartitionsRequestTopic{t, u}
		r.TimeoutMillis, r.ValidateOnly = 30000, true
		req("create_partitions", r, 2)
	}
	{
		r := kmsg.NewPtrDescribeConfigsRequest()
		a := kmsg.NewDescribeConfigsRequestResource()
		a.ResourceType, a.ResourceName, a.ConfigNames = 2, "t", []string{"retention.ms"}
		b := kmsg.NewDescribeConfigsRequestResource()
		b.ResourceType, b.ResourceName, b.ConfigNames = 4, "1", nil
		r.Resources = []kmsg.DescribeConfigsRequestResource{a, b}
		req("describe_configs", r, 4)
	}
	{
		r := kmsg.NewPtrIncrementalAlterConfigsRequest()
		a := kmsg.NewIncrementalAlterConfigsRequestResource()
		a.ResourceType, a.ResourceName = 2, "t"
		c1 := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
		c1.Name, c1.Op, c1.Value = "retention.ms", 0, sp("1000")
		c2 := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
		c2.Name, c2.Op, c2.Value = "cleanup.policy", 1, nil
		a.Configs = []kmsg.IncrementalAlterConfigsRequestResourceConfig{c1, c2}
		r.Resources = []kmsg.IncrementalAlterConfigsRequestResource{a}
		req("alter_configs", r, 1)
	}
	{
		r := kmsg.NewPtrInitProducerIDRequest()
		r.TransactionalID, r.TransactionTimeoutMillis, r.ProducerID, r.ProducerEpoch = nil, 60000, -1, -1
		req("init_producer_id", r, 4)
		r2 := kmsg.NewPtrInitProducerIDRequest()
		r2.TransactionalID, r2.TransactionTimeoutMillis, r2.ProducerID, r2.ProducerEpoch = sp("tx"), 60000, 5, 2
		req("init_producer_id_txn", r2, 4)
	}
	{
		r := kmsg.NewPtrAddPartitionsToTxnRequest()
		r.TransactionalID, r.ProducerID, r.ProducerEpoch = "tx", 5, 2
		for _, x := range []struct {
			t  string
			ps []int32
		}{{"t", []int32{0, 1}}, {"u", []int32{3}}} {
			t := kmsg.NewAddPartitionsToTxnRequestTopic()
			t.Topic, t.Partitions = x.t, x.ps
			r.Topics = append(r.Topics, t)
		}
		req("add_partitions_to_txn", r, 3)
		a := kmsg.NewPtrAddOffsetsToTxnRequest()
		a.TransactionalID, a.ProducerID, a.ProducerEpoch, a.Group = "tx", 5, 2, "g"
		req("add_offsets_to_txn", a, 3)
		e := kmsg.NewPtrEndTxnRequest()
		e.TransactionalID, e.ProducerID, e.ProducerEpoch, e.Commit = "tx", 5, 2, true
		req("end_txn", e, 3)
		c := kmsg.NewPtrTxnOffsetCommitRequest()
		c.TransactionalID, c.Group, c.ProducerID, c.ProducerEpoch, c.Generation, c.MemberID = "tx", "g", 5, 2, 3, "m"
		for _, t := range commits() {
			tt := kmsg.NewTxnOffsetCommitRequestTopic()
			tt.Topic = t.Topic
			for _, p := range t.Partitions {
				pp := kmsg.NewTxnOffsetCommitRequestTopicPartition()
				pp.Partition, pp.Offset, pp.LeaderEpoch, pp.Metadata = p.Partition, p.Offset, p.LeaderEpoch, p.Metadata
				tt.Partitions = append(tt.Partitions, pp)
			}
			c.Topics = append(c.Topics, tt)
		}
		req("txn_offset_commit", c, 3)
	}
}

// The classic versions sent to brokers without the flexible ones
// (Redpanda): Produce v7, Fetch v11, ListOffsets v4, Metadata v8,
// AddOffsetsToTxn v1.
func fallbackRequests() {
	m := kmsg.NewPtrMetadataRequest()
	for _, t := range []string{"a", "bb"} {
		mt := kmsg.NewMetadataRequestTopic()
		mt.Topic = sp(t)
		m.Topics = append(m.Topics, mt)
	}
	req("metadata_v8", m, 8)
	p := kmsg.NewPtrProduceRequest()
	p.TransactionID, p.Acks, p.TimeoutMillis = sp("tx"), -1, 30000
	t := kmsg.NewProduceRequestTopic()
	t.Topic = "t1"
	pp := kmsg.NewProduceRequestTopicPartition()
	pp.Partition, pp.Records = 3, []byte("batch")
	t.Partitions = []kmsg.ProduceRequestTopicPartition{pp}
	p.Topics = []kmsg.ProduceRequestTopic{t}
	req("produce_v7", p, 7)
	f := kmsg.NewPtrFetchRequest()
	f.ReplicaID, f.MaxWaitMillis, f.MinBytes, f.MaxBytes, f.IsolationLevel, f.SessionID, f.SessionEpoch = -1, 500, 1, 1000, 0, 0, -1
	ft := kmsg.NewFetchRequestTopic()
	ft.Topic = "t1"
	fp := kmsg.NewFetchRequestTopicPartition()
	fp.Partition, fp.CurrentLeaderEpoch, fp.FetchOffset, fp.LogStartOffset, fp.PartitionMaxBytes = 2, -1, 77, -1, 4096
	ft.Partitions = []kmsg.FetchRequestTopicPartition{fp}
	f.Topics = []kmsg.FetchRequestTopic{ft}
	f.Rack = "r"
	req("fetch_v11", f, 11)
	l := kmsg.NewPtrListOffsetsRequest()
	l.ReplicaID, l.IsolationLevel = -1, 0
	lt := kmsg.NewListOffsetsRequestTopic()
	lt.Topic = "t1"
	lp := kmsg.NewListOffsetsRequestTopicPartition()
	lp.Partition, lp.CurrentLeaderEpoch, lp.Timestamp = 0, -1, -2
	lt.Partitions = []kmsg.ListOffsetsRequestTopicPartition{lp}
	l.Topics = []kmsg.ListOffsetsRequestTopic{lt}
	req("list_offsets_v4", l, 4)
	a := kmsg.NewPtrAddOffsetsToTxnRequest()
	a.TransactionalID, a.ProducerID, a.ProducerEpoch, a.Group = "tx", 5, 2, "g"
	req("add_offsets_to_txn_v1", a, 1)
}

func fallbackResponses() {
	m := kmsg.NewPtrMetadataResponse()
	b := kmsg.NewMetadataResponseBroker()
	b.NodeID, b.Host, b.Port = 0, "rp", 9092
	m.Brokers = []kmsg.MetadataResponseBroker{b}
	m.ClusterID, m.ControllerID = sp("redpanda"), 0
	mt := kmsg.NewMetadataResponseTopic()
	mt.Topic = sp("t")
	mp := kmsg.NewMetadataResponseTopicPartition()
	mp.Partition, mp.Leader, mp.LeaderEpoch, mp.Replicas, mp.ISR = 0, 0, 1, []int32{0}, []int32{0}
	mt.Partitions = []kmsg.MetadataResponseTopicPartition{mp}
	m.Topics = []kmsg.MetadataResponseTopic{mt}
	resp("metadata_v8", m, 8, "cluster=redpanda controller=0", "broker 0 rp:9092 rack=null", "topic t error=0 internal=false",
		"  part 0 error=0 leader=0 epoch=1 replicas=[0] isr=[0] offline=[]")
	p := kmsg.NewPtrProduceResponse()
	pt := kmsg.NewProduceResponseTopic()
	pt.Topic = "t"
	pp := kmsg.NewProduceResponseTopicPartition()
	pp.Partition, pp.BaseOffset, pp.LogAppendTime, pp.LogStartOffset = 3, 42, -1, 0
	pt.Partitions = []kmsg.ProduceResponseTopicPartition{pp}
	p.Topics = []kmsg.ProduceResponseTopic{pt}
	resp("produce_v7", p, 7, "throttle=0", "part t/3 error=0 base=42 lat=-1 lso=0 msg=")
	f := kmsg.NewPtrFetchResponse()
	ft := kmsg.NewFetchResponseTopic()
	ft.Topic = "t"
	fp := kmsg.NewFetchResponseTopicPartition()
	fp.Partition, fp.HighWatermark, fp.LastStableOffset, fp.LogStartOffset, fp.PreferredReadReplica = 0, 9, 9, 0, -1
	ab := kmsg.NewFetchResponseTopicPartitionAbortedTransaction()
	ab.ProducerID, ab.FirstOffset = 7, 3
	fp.AbortedTransactions = []kmsg.FetchResponseTopicPartitionAbortedTransaction{ab}
	fp.RecordBatches = []byte("abc")
	ft.Partitions = []kmsg.FetchResponseTopicPartition{fp}
	f.Topics = []kmsg.FetchResponseTopic{ft}
	resp("fetch_v11", f, 11, "error=0 session=0", "part t/0 error=0 hw=9 lso=9 start=0 aborted=[7@3] preferred=-1 records=abc")
	l := kmsg.NewPtrListOffsetsResponse()
	lt := kmsg.NewListOffsetsResponseTopic()
	lt.Topic = "t"
	lp := kmsg.NewListOffsetsResponseTopicPartition()
	lp.Partition, lp.Timestamp, lp.Offset, lp.LeaderEpoch = 0, -1, 5, 1
	lt.Partitions = []kmsg.ListOffsetsResponseTopicPartition{lp}
	l.Topics = []kmsg.ListOffsetsResponseTopic{lt}
	resp("list_offsets_v4", l, 4, "offset t/0 error=0 ts=-1 offset=5 epoch=1")
	a := kmsg.NewPtrAddOffsetsToTxnResponse()
	a.ErrorCode = 51
	resp("add_offsets_to_txn_v1", a, 1, "error=51")
}

func responses() {
	{
		r := kmsg.NewPtrApiVersionsResponse()
		for _, k := range [][3]int16{{0, 3, 9}, {1, 4, 12}, {18, 0, 3}} {
			a := kmsg.NewApiVersionsResponseApiKey()
			a.ApiKey, a.MinVersion, a.MaxVersion = k[0], k[1], k[2]
			r.ApiKeys = append(r.ApiKeys, a)
		}
		r.ThrottleMillis = 5
		resp("api_versions", r, 3, "error=0 throttle=5", "api 0 3..9", "api 1 4..12", "api 18 0..3")
		old := kmsg.NewPtrApiVersionsResponse()
		old.ErrorCode = 35
		a := kmsg.NewApiVersionsResponseApiKey()
		a.ApiKey, a.MinVersion, a.MaxVersion = 18, 0, 2
		old.ApiKeys = append(old.ApiKeys, a)
		resp("api_versions_v0", old, 0, "error=35 throttle=0", "api 18 0..2")
	}
	{
		r := kmsg.NewPtrMetadataResponse()
		b1 := kmsg.NewMetadataResponseBroker()
		b1.NodeID, b1.Host, b1.Port, b1.Rack = 1, "b1", 9092, sp("r1")
		b2 := kmsg.NewMetadataResponseBroker()
		b2.NodeID, b2.Host, b2.Port = 2, "b2", 9093
		r.Brokers = []kmsg.MetadataResponseBroker{b1, b2}
		r.ClusterID, r.ControllerID = sp("cid"), 1
		t := kmsg.NewMetadataResponseTopic()
		t.Topic = sp("t")
		p := kmsg.NewMetadataResponseTopicPartition()
		p.Partition, p.Leader, p.LeaderEpoch, p.Replicas, p.ISR, p.OfflineReplicas = 0, 1, 4, []int32{1, 2}, []int32{1}, []int32{2}
		t.Partitions = []kmsg.MetadataResponseTopicPartition{p}
		m := kmsg.NewMetadataResponseTopic()
		m.Topic, m.ErrorCode = sp("missing"), 3
		r.Topics = []kmsg.MetadataResponseTopic{t, m}
		resp("metadata", r, 9, "cluster=cid controller=1", "broker 1 b1:9092 rack=r1", "broker 2 b2:9093 rack=null",
			"topic t error=0 internal=false", "  part 0 error=0 leader=1 epoch=4 replicas=[1 2] isr=[1] offline=[2]",
			"topic missing error=3 internal=false")
	}
	{
		r := kmsg.NewPtrFindCoordinatorResponse()
		r.ErrorCode, r.ErrorMessage, r.NodeID, r.Host, r.Port = 15, sp("not available"), -1, "", -1
		resp("find_coordinator_error", r, 3, "error=15 msg=not available node=-1 :-1")
		ok := kmsg.NewPtrFindCoordinatorResponse()
		ok.NodeID, ok.Host, ok.Port = 2, "b2", 9093
		resp("find_coordinator", ok, 3, "error=0 msg= node=2 b2:9093")
	}
	{
		r := kmsg.NewPtrSASLHandshakeResponse()
		r.ErrorCode, r.SupportedMechanisms = 33, []string{"PLAIN", "SCRAM-SHA-256"}
		resp("sasl_handshake", r, 1, "error=33 mechanisms=[PLAIN SCRAM-SHA-256]")
		a := kmsg.NewPtrSASLAuthenticateResponse()
		a.SASLAuthBytes, a.SessionLifetimeMillis = []byte("r=abc"), 3600000
		resp("sasl_authenticate", a, 2, "error=0 msg= auth=r=abc lifetime=3600000")
	}
	{
		r := kmsg.NewPtrProduceResponse()
		t := kmsg.NewProduceResponseTopic()
		t.Topic = "t"
		p0 := kmsg.NewProduceResponseTopicPartition()
		p0.Partition, p0.BaseOffset, p0.LogAppendTime, p0.LogStartOffset = 0, 100, -1, 0
		p1 := kmsg.NewProduceResponseTopicPartition()
		p1.Partition, p1.ErrorCode, p1.BaseOffset, p1.ErrorMessage = 1, 87, -1, sp("bad record")
		e := kmsg.NewProduceResponseTopicPartitionErrorRecord()
		e.RelativeOffset, e.ErrorMessage = 2, sp("null key on a compacted topic")
		p1.ErrorRecords = []kmsg.ProduceResponseTopicPartitionErrorRecord{e}
		t.Partitions = []kmsg.ProduceResponseTopicPartition{p0, p1}
		r.Topics = []kmsg.ProduceResponseTopic{t}
		r.ThrottleMillis = 3
		resp("produce", r, 9, "throttle=3", "part t/0 error=0 base=100 lat=-1 lso=0 msg=",
			"part t/1 error=87 base=-1 lat=-1 lso=-1 msg=bad record", "  record 2 null key on a compacted topic")
	}
	{
		r := kmsg.NewPtrFetchResponse()
		r.SessionID = 7
		t := kmsg.NewFetchResponseTopic()
		t.Topic = "t"
		p0 := kmsg.NewFetchResponseTopicPartition()
		p0.Partition, p0.HighWatermark, p0.LastStableOffset, p0.LogStartOffset = 0, 120, 118, 0
		a := kmsg.NewFetchResponseTopicPartitionAbortedTransaction()
		a.ProducerID, a.FirstOffset = 5, 100
		p0.AbortedTransactions = []kmsg.FetchResponseTopicPartitionAbortedTransaction{a}
		p0.PreferredReadReplica = -1
		p0.RecordBatches = []byte("xyz")
		p1 := kmsg.NewFetchResponseTopicPartition()
		p1.Partition, p1.ErrorCode, p1.HighWatermark, p1.LastStableOffset, p1.LogStartOffset = 1, 1, -1, -1, -1
		p1.PreferredReadReplica = 3
		p1.CurrentLeader.LeaderID, p1.CurrentLeader.LeaderEpoch = 2, 9
		t.Partitions = []kmsg.FetchResponseTopicPartition{p0, p1}
		r.Topics = []kmsg.FetchResponseTopic{t}
		resp("fetch", r, 12, "error=0 session=7",
			"part t/0 error=0 hw=120 lso=118 start=0 aborted=[5@100] preferred=-1 records=xyz",
			"part t/1 error=1 hw=-1 lso=-1 start=-1 aborted=null preferred=3 records=")
	}
	{
		r := kmsg.NewPtrListOffsetsResponse()
		t := kmsg.NewListOffsetsResponseTopic()
		t.Topic = "t"
		p := kmsg.NewListOffsetsResponseTopicPartition()
		p.Partition, p.Timestamp, p.Offset, p.LeaderEpoch = 0, -1, 100, 3
		q := kmsg.NewListOffsetsResponseTopicPartition()
		q.Partition, q.ErrorCode, q.Timestamp, q.Offset, q.LeaderEpoch = 1, 6, -1, -1, -1
		t.Partitions = []kmsg.ListOffsetsResponseTopicPartition{p, q}
		r.Topics = []kmsg.ListOffsetsResponseTopic{t}
		resp("list_offsets", r, 6, "offset t/0 error=0 ts=-1 offset=100 epoch=3", "offset t/1 error=6 ts=-1 offset=-1 epoch=-1")
	}
	{
		r := kmsg.NewPtrJoinGroupResponse()
		r.Generation, r.Protocol, r.LeaderID, r.MemberID = 3, sp("range"), "m1", "m1"
		m1 := kmsg.NewJoinGroupResponseMember()
		m1.MemberID, m1.InstanceID, m1.ProtocolMetadata = "m1", sp("i1"), []byte("meta1")
		m2 := kmsg.NewJoinGroupResponseMember()
		m2.MemberID, m2.ProtocolMetadata = "m2", []byte("meta2")
		r.Members = []kmsg.JoinGroupResponseMember{m1, m2}
		resp("join_group", r, 6, "error=0 generation=3 protocol=range leader=m1 member=m1",
			"member m1 instance=i1 meta=meta1", "member m2 instance=null meta=meta2")
	}
	{
		r := kmsg.NewPtrSyncGroupResponse()
		r.MemberAssignment = []byte("assignment")
		resp("sync_group", r, 4, "error=0 assignment=assignment")
		h := kmsg.NewPtrHeartbeatResponse()
		h.ErrorCode = 27
		resp("heartbeat", h, 4, "error=27")
		l := kmsg.NewPtrLeaveGroupResponse()
		m := kmsg.NewLeaveGroupResponseMember()
		m.MemberID, m.ErrorCode = "m", 25
		l.Members = []kmsg.LeaveGroupResponseMember{m}
		resp("leave_group", l, 4, "error=25")
	}
	{
		r := kmsg.NewPtrOffsetCommitResponse()
		t := kmsg.NewOffsetCommitResponseTopic()
		t.Topic = "t"
		for _, x := range [][2]int32{{0, 0}, {1, 22}} {
			p := kmsg.NewOffsetCommitResponseTopicPartition()
			p.Partition, p.ErrorCode = x[0], int16(x[1])
			t.Partitions = append(t.Partitions, p)
		}
		r.Topics = []kmsg.OffsetCommitResponseTopic{t}
		resp("offset_commit", r, 8, "part t/0 error=0", "part t/1 error=22")
	}
	{
		r := kmsg.NewPtrOffsetFetchResponse()
		t := kmsg.NewOffsetFetchResponseTopic()
		t.Topic = "t"
		p := kmsg.NewOffsetFetchResponseTopicPartition()
		p.Partition, p.Offset, p.LeaderEpoch, p.Metadata = 0, 100, 2, sp("x")
		q := kmsg.NewOffsetFetchResponseTopicPartition()
		q.Partition, q.Offset, q.LeaderEpoch = 1, -1, -1
		t.Partitions = []kmsg.OffsetFetchResponseTopicPartition{p, q}
		r.Topics = []kmsg.OffsetFetchResponseTopic{t}
		resp("offset_fetch", r, 7, "error=0", "committed t/0 offset=100 epoch=2 meta=x error=0",
			"committed t/1 offset=-1 epoch=-1 meta= error=0")
	}
	{
		r := kmsg.NewPtrDescribeGroupsResponse()
		g := kmsg.NewDescribeGroupsResponseGroup()
		g.Group, g.State, g.ProtocolType, g.Protocol = "g", "Stable", "consumer", "range"
		m := kmsg.NewDescribeGroupsResponseGroupMember()
		m.MemberID, m.ClientID, m.ClientHost, m.ProtocolMetadata, m.MemberAssignment = "m1", "c", "/10.0.0.1", []byte("md"), []byte("as")
		g.Members = []kmsg.DescribeGroupsResponseGroupMember{m}
		r.Groups = []kmsg.DescribeGroupsResponseGroup{g}
		resp("describe_groups", r, 5, "group g error=0 state=Stable type=consumer protocol=range",
			"  member m1 instance=null client=c host=/10.0.0.1 meta=md assignment=as")
		l := kmsg.NewPtrListGroupsResponse()
		for _, x := range [][2]string{{"g", "consumer"}, {"h", ""}} {
			lg := kmsg.NewListGroupsResponseGroup()
			lg.Group, lg.ProtocolType = x[0], x[1]
			l.Groups = append(l.Groups, lg)
		}
		resp("list_groups", l, 3, "error=0", "group g type=consumer", "group h type=")
		d := kmsg.NewPtrDeleteGroupsResponse()
		for _, x := range []struct {
			g string
			c int16
		}{{"g", 0}, {"h", 69}} {
			dg := kmsg.NewDeleteGroupsResponseGroup()
			dg.Group, dg.ErrorCode = x.g, x.c
			d.Groups = append(d.Groups, dg)
		}
		resp("delete_groups", d, 2, "result g error=0 msg=", "result h error=69 msg=")
	}
	{
		r := kmsg.NewPtrCreateTopicsResponse()
		t := kmsg.NewCreateTopicsResponseTopic()
		t.Topic, t.NumPartitions, t.ReplicationFactor = "t", 3, 2
		c := kmsg.NewCreateTopicsResponseTopicConfig()
		c.Name, c.Value, c.Source = "cleanup.policy", sp("compact"), 1
		t.Configs = []kmsg.CreateTopicsResponseTopicConfig{c}
		u := kmsg.NewCreateTopicsResponseTopic()
		u.Topic, u.ErrorCode, u.ErrorMessage, u.NumPartitions, u.ReplicationFactor = "u", 36, sp("exists"), -1, -1
		u.Configs = nil
		r.Topics = []kmsg.CreateTopicsResponseTopic{t, u}
		resp("create_topics", r, 5, "topic t error=0 msg= partitions=3 rf=2", "topic u error=36 msg=exists partitions=-1 rf=-1")
		d := kmsg.NewPtrDeleteTopicsResponse()
		for _, x := range []struct {
			t string
			c int16
		}{{"t", 0}, {"u", 3}} {
			dt := kmsg.NewDeleteTopicsResponseTopic()
			dt.Topic, dt.ErrorCode = sp(x.t), x.c
			d.Topics = append(d.Topics, dt)
		}
		resp("delete_topics", d, 4, "result t error=0 msg=", "result u error=3 msg=")
	}
	{
		r := kmsg.NewPtrDeleteRecordsResponse()
		t := kmsg.NewDeleteRecordsResponseTopic()
		t.Topic = "t"
		p := kmsg.NewDeleteRecordsResponseTopicPartition()
		p.Partition, p.LowWatermark = 0, 100
		t.Partitions = []kmsg.DeleteRecordsResponseTopicPartition{p}
		r.Topics = []kmsg.DeleteRecordsResponseTopic{t}
		resp("delete_records", r, 2, "deleted t/0 low=100 error=0")
		c := kmsg.NewPtrCreatePartitionsResponse()
		for _, x := range []struct {
			t string
			c int16
			m *string
		}{{"t", 0, nil}, {"u", 37, sp("bad")}} {
			ct := kmsg.NewCreatePartitionsResponseTopic()
			ct.Topic, ct.ErrorCode, ct.ErrorMessage = x.t, x.c, x.m
			c.Topics = append(c.Topics, ct)
		}
		resp("create_partitions", c, 2, "result t error=0 msg=", "result u error=37 msg=bad")
	}
	{
		r := kmsg.NewPtrDescribeConfigsResponse()
		res := kmsg.NewDescribeConfigsResponseResource()
		res.ResourceType, res.ResourceName = 2, "t"
		c := kmsg.NewDescribeConfigsResponseResourceConfig()
		c.Name, c.Value, c.Source, c.ConfigType = "retention.ms", sp("1000"), 1, 5
		s := kmsg.NewDescribeConfigsResponseResourceConfigConfigSynonym()
		s.Name, s.Value, s.Source = "log.retention.ms", sp("2000"), 5
		c.ConfigSynonyms = []kmsg.DescribeConfigsResponseResourceConfigConfigSynonym{s}
		d := kmsg.NewDescribeConfigsResponseResourceConfig()
		d.Name, d.Value, d.ReadOnly, d.Source, d.IsSensitive = "sasl.jaas.config", nil, true, 4, true
		res.Configs = []kmsg.DescribeConfigsResponseResourceConfig{c, d}
		r.Resources = []kmsg.DescribeConfigsResponseResource{res}
		resp("describe_configs", r, 4, "resource 2 t error=0 msg=", "  config retention.ms=1000 ro=false source=1 sensitive=false",
			"  config sasl.jaas.config=null ro=true source=4 sensitive=true")
		a := kmsg.NewPtrIncrementalAlterConfigsResponse()
		ar := kmsg.NewIncrementalAlterConfigsResponseResource()
		ar.ResourceType, ar.ResourceName, ar.ErrorCode, ar.ErrorMessage = 2, "t", 40, sp("bad value")
		a.Resources = []kmsg.IncrementalAlterConfigsResponseResource{ar}
		resp("alter_configs", a, 1, "result t error=40 msg=bad value")
	}
	{
		r := kmsg.NewPtrInitProducerIDResponse()
		r.ProducerID, r.ProducerEpoch = 5, 2
		resp("init_producer_id", r, 4, "error=0 pid=5 epoch=2")
		a := kmsg.NewPtrAddPartitionsToTxnResponse()
		t := kmsg.NewAddPartitionsToTxnResponseTopic()
		t.Topic = "t"
		for _, x := range [][2]int32{{0, 0}, {1, 51}} {
			p := kmsg.NewAddPartitionsToTxnResponseTopicPartition()
			p.Partition, p.ErrorCode = x[0], int16(x[1])
			t.Partitions = append(t.Partitions, p)
		}
		a.Topics = []kmsg.AddPartitionsToTxnResponseTopic{t}
		resp("add_partitions_to_txn", a, 3, "part t/0 error=0", "part t/1 error=51")
		o := kmsg.NewPtrAddOffsetsToTxnResponse()
		resp("add_offsets_to_txn", o, 3, "error=0")
		e := kmsg.NewPtrEndTxnResponse()
		e.ErrorCode = 48
		resp("end_txn", e, 3, "error=48")
		c := kmsg.NewPtrTxnOffsetCommitResponse()
		ct := kmsg.NewTxnOffsetCommitResponseTopic()
		ct.Topic = "t"
		p := kmsg.NewTxnOffsetCommitResponseTopicPartition()
		p.Partition = 0
		ct.Partitions = []kmsg.TxnOffsetCommitResponseTopicPartition{p}
		c.Topics = []kmsg.TxnOffsetCommitResponseTopic{ct}
		resp("txn_offset_commit", c, 3, "part t/0 error=0")
	}
}

func main() {
	requests()
	// the header around a request: api key, version, correlation id,
	// client id (a classic nullable string), tagged fields
	hdr := []byte{0, 0, 0, 0, 0, 18, 0, 3, 0, 0, 0, 7, 0, 6, 'c', 'l', 'i', 'e', 'n', 't', 0}
	body := reqs[0].data
	frame := append(append([]byte{}, hdr...), body...)
	size := len(frame) - 4
	frame[0], frame[1], frame[2], frame[3] = byte(size>>24), byte(size>>16), byte(size>>8), byte(size)
	reqs = append(reqs, vec{"frame_api_versions", frame})
	fmt.Fprintf(&want, "req frame_api_versions same\n")
	fallbackRequests()
	responses()
	fallbackResponses()
	var src strings.Builder
	src.WriteString("// Generated by tests/protocol/gen (go run .); do not edit.\n\n")
	for _, set := range []struct {
		fn string
		vs []vec
	}{{"request_vectors", reqs}, {"response_vectors", resps}} {
		fmt.Fprintf(&src, "fn %s() -> map[str]str {\n    let out: map[str]str = {};\n", set.fn)
		for _, v := range set.vs {
			fmt.Fprintf(&src, "    out[%q] = %q;\n", v.name, hex.EncodeToString(v.data))
		}
		src.WriteString("    return out;\n}\n\n")
	}
	must(os.WriteFile("../vectors.sl", []byte(src.String()), 0o644))
	fmt.Fprintf(&want, "truncated %d of %d responses refused\n", 2*len(resps), 2*len(resps))
	must(os.WriteFile("../expected.txt", []byte(want.String()), 0o644))
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

var _ = ns
