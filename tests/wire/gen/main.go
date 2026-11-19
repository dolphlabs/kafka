// Writes ../vectors.sl and ../expected.txt: record batches (v2) and
// message sets (v0, v1) built with franz-go's kmsg, and varints from
// encoding/binary. expected.txt is what each vector holds, written from
// the specs below, not by decoding the bytes: the slang decoder is checked
// against an independent writer.
//
//	cd tests/wire/gen && go run .
package main

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kmsg"
)

type rec struct {
	tsDelta int64
	offDelta int32
	key     []byte
	value   []byte
	headers []kmsg.Header
}

type batch struct {
	baseOffset  int64
	leaderEpoch int32
	attrs       int16
	firstTs     int64
	maxTs       int64
	pid         int64
	epoch       int16
	seq         int32
	lastDelta   int32 // -1: from the records
	recs        []rec
	gzip        bool
	control     int // -1 none, else the control type
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func encodeRecords(b batch) []byte {
	var out []byte
	for _, r := range b.recs {
		kr := kmsg.Record{TimestampDelta64: r.tsDelta, OffsetDelta: r.offDelta, Key: r.key, Value: r.value, Headers: r.headers}
		body := kr.AppendTo(nil) // Length 0: one byte
		kr.Length = int32(len(body) - 1)
		out = kr.AppendTo(out)
	}
	return out
}

func encodeBatch(b batch) []byte {
	records := encodeRecords(b)
	attrs := b.attrs
	if b.gzip {
		var z bytes.Buffer
		w := gzip.NewWriter(&z)
		w.Write(records)
		w.Close()
		records = z.Bytes()
		attrs |= 1
	}
	last := b.lastDelta
	if last < 0 {
		last = b.recs[len(b.recs)-1].offDelta
	}
	kb := kmsg.RecordBatch{FirstOffset: b.baseOffset, PartitionLeaderEpoch: b.leaderEpoch, Magic: 2, Attributes: attrs,
		LastOffsetDelta: last, FirstTimestamp: b.firstTs, MaxTimestamp: b.maxTs, ProducerID: b.pid,
		ProducerEpoch: b.epoch, FirstSequence: b.seq, NumRecords: int32(len(b.recs)), Records: records}
	raw := kb.AppendTo(nil)
	binary.BigEndian.PutUint32(raw[8:], uint32(len(raw)-12))
	binary.BigEndian.PutUint32(raw[17:], crc32.Checksum(raw[21:], castagnoli))
	return raw
}

func short(b []byte) string {
	if b == nil {
		return "null"
	}
	if len(b) <= 32 {
		return hex.EncodeToString(b)
	}
	s := sha256.Sum256(b)
	return fmt.Sprintf("#%d:%s", len(b), hex.EncodeToString(s[:8]))
}

func headersLine(hs []kmsg.Header) string {
	parts := []string{}
	for _, h := range hs {
		parts = append(parts, h.Key+"="+short(h.Value))
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func describe(b batch) []string {
	codec := 0
	if b.gzip {
		codec = 1
	}
	txn := b.attrs&16 != 0
	ctl := b.attrs&32 != 0
	lat := b.attrs&8 != 0
	last := b.lastDelta
	if last < 0 {
		last = b.recs[len(b.recs)-1].offDelta
	}
	lines := []string{fmt.Sprintf("  b base=%d magic=2 codec=%d txn=%v control=%v pid=%d epoch=%d seq=%d count=%d next=%d",
		b.baseOffset, codec, txn, ctl, b.pid, b.epoch, b.seq, len(b.recs), b.baseOffset+int64(last)+1)}
	if ctl {
		lines = append(lines, fmt.Sprintf("  control type=%d", b.control))
		return lines
	}
	for _, r := range b.recs {
		ts := b.firstTs + r.tsDelta
		if lat {
			ts = b.maxTs
		}
		lines = append(lines, fmt.Sprintf("  m off=%d ts=%d lat=%v key=%s value=%s h=%s", b.baseOffset+int64(r.offDelta),
			ts, lat, short(r.key), short(r.value), headersLine(r.headers)))
	}
	return lines
}

// ---- v0 / v1 ----------------------------------------------------------------

type legacy struct {
	magic  int8
	offset int64
	attrs  int8
	ts     int64
	key    []byte
	value  []byte
}

func encodeLegacy(m legacy) []byte {
	var raw []byte
	if m.magic == 0 {
		km := kmsg.MessageV0{Offset: m.offset, Magic: 0, Attributes: m.attrs, Key: m.key, Value: m.value}
		raw = km.AppendTo(nil)
	} else {
		km := kmsg.MessageV1{Offset: m.offset, Magic: 1, Attributes: m.attrs, Timestamp: m.ts, Key: m.key, Value: m.value}
		raw = km.AppendTo(nil)
	}
	binary.BigEndian.PutUint32(raw[8:], uint32(len(raw)-12))
	binary.BigEndian.PutUint32(raw[12:], crc32.ChecksumIEEE(raw[16:]))
	return raw
}

func gz(b []byte) []byte {
	var z bytes.Buffer
	w := gzip.NewWriter(&z)
	w.Write(b)
	w.Close()
	return z.Bytes()
}

func legacyLine(magic int, off int64, ts int64, lat bool, key, value []byte) string {
	return fmt.Sprintf("  m off=%d ts=%d lat=%v key=%s value=%s h=[]", off, ts, lat, short(key), short(value))
}

func legacyHead(m legacy, codec int) string {
	return fmt.Sprintf("  b base=%d magic=%d codec=%d txn=false control=false pid=-1 epoch=-1 seq=-1 count=1 next=%d",
		m.offset, m.magic, codec, m.offset+1)
}

// ---- output -----------------------------------------------------------------

type vec struct {
	name     string
	data     []byte
	reencode bool
	expect   []string
}

func bytesOf(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7) + seed
	}
	return b
}

func main() {
	const base = 1700000000000
	var vecs []vec
	add := func(name string, b batch, reencode bool) {
		vecs = append(vecs, vec{name, encodeBatch(b), reencode, describe(b)})
	}
	plain := func(base0 int64, recs []rec) batch {
		var maxDelta int64
		for _, r := range recs {
			if r.tsDelta > maxDelta {
				maxDelta = r.tsDelta
			}
		}
		return batch{baseOffset: base0, leaderEpoch: -1, firstTs: base, maxTs: base + maxDelta, pid: -1, epoch: -1,
			seq: -1, lastDelta: -1, recs: recs, control: -1}
	}

	add("one", plain(0, []rec{{0, 0, []byte("k"), []byte("v"), nil}}), true)
	add("nulls", plain(0, []rec{{0, 0, nil, nil, nil}, {0, 1, []byte{}, []byte{}, nil}}), true)
	add("headers", plain(0, []rec{{5, 0, []byte("id-1"), []byte(`{"a":1}`), []kmsg.Header{
		{Key: "content-type", Value: []byte("application/json")}, {Key: "trace", Value: nil}, {Key: "", Value: []byte{}}}}}), true)
	var many []rec
	for i := 0; i < 50; i++ {
		var k []byte
		if i%7 != 3 {
			k = []byte(fmt.Sprintf("key-%d", i))
		}
		v := bytesOf([]int{0, 1, 63, 64, 127, 128, 129, 300, 16383, 16384, 20000}[i%11], byte(i))
		var hs []kmsg.Header
		if i%5 == 0 {
			hs = []kmsg.Header{{Key: fmt.Sprintf("h%d", i), Value: bytesOf(i, 1)}}
		}
		many = append(many, rec{int64(i*37) - 100, int32(i), k, v, hs})
	}
	mb := plain(5000, many)
	mb.maxTs = base + 49*37 - 100
	add("many", mb, true)
	pb := plain(1000, []rec{{0, 0, []byte("a"), []byte("1"), nil}, {1, 1, []byte("b"), []byte("2"), nil}})
	pb.pid, pb.epoch, pb.seq, pb.leaderEpoch, pb.attrs = 12345, 3, 17, 9, 16
	add("producer", pb, true)
	for _, ct := range []int{1, 0} {
		cb := plain(1002, []rec{{0, 0, []byte{0, 0, 0, byte(ct)}, []byte{0, 0, 0, 0, 0, 5}, nil}})
		cb.pid, cb.epoch, cb.attrs, cb.control = 12345, 3, 16|32, ct
		name := "control_commit"
		if ct == 0 {
			name = "control_abort"
		}
		add(name, cb, true)
	}
	lb := plain(70, []rec{{0, 0, nil, []byte("x"), nil}, {9, 1, nil, []byte("y"), nil}})
	lb.attrs, lb.maxTs = 8, base+12345
	add("log_append_time", lb, false)
	gb := plain(200, []rec{{0, 0, []byte("a"), []byte("first"), nil}, {3, 2, []byte("b"), nil, nil}, {4, 5, []byte("c"), []byte("third"), nil}})
	gb.lastDelta = 7
	add("compacted_gaps", gb, false)
	zb := plain(300, []rec{{0, 0, []byte("g1"), bytes.Repeat([]byte("gzip "), 100), nil}, {1, 1, nil, []byte("small"), nil}})
	zb.gzip = true
	add("gzip", zb, false)

	// two batches, then the first 30 bytes of a third: a fetch cut off mid-batch
	b1 := plain(10, []rec{{0, 0, nil, []byte("one"), nil}})
	b2 := plain(11, []rec{{0, 0, nil, []byte("two"), nil}, {1, 1, nil, []byte("three"), nil}})
	b3 := encodeBatch(plain(13, []rec{{0, 0, nil, []byte("four"), nil}}))
	data := append(append(encodeBatch(b1), encodeBatch(b2)...), b3[:30]...)
	vecs = append(vecs, vec{"partial_tail", data, false, append(append(describe(b1), describe(b2)...), "  partial")})

	bad := encodeBatch(plain(0, []rec{{0, 0, []byte("k"), []byte("value"), nil}}))
	bad[len(bad)-3] ^= 0xff
	vecs = append(vecs, vec{"bad_crc", bad, false, []string{"  error"}})

	cnt := encodeBatch(plain(0, []rec{{0, 0, []byte("k"), []byte("v"), nil}}))
	binary.BigEndian.PutUint32(cnt[57:], 1000000)
	binary.BigEndian.PutUint32(cnt[17:], crc32.Checksum(cnt[21:], castagnoli))
	vecs = append(vecs, vec{"bad_count", cnt, false, []string{
		"  b base=0 magic=2 codec=0 txn=false control=false pid=-1 epoch=-1 seq=-1 count=1000000 next=1", "  error"}})

	ln := encodeBatch(plain(0, []rec{{0, 0, []byte("k"), []byte("v"), nil}}))
	ln[61] += 2 // the record's length varint (zigzag): one more byte than it has
	binary.BigEndian.PutUint32(ln[17:], crc32.Checksum(ln[21:], castagnoli))
	vecs = append(vecs, vec{"bad_record_length", ln, false, []string{
		"  b base=0 magic=2 codec=0 txn=false control=false pid=-1 epoch=-1 seq=-1 count=1 next=1", "  error"}})

	// v0 and v1
	v0a := legacy{0, 40, 0, -1, []byte("k0"), []byte("v0")}
	v0b := legacy{0, 41, 0, -1, nil, nil}
	vecs = append(vecs, vec{"v0", append(encodeLegacy(v0a), encodeLegacy(v0b)...), false, []string{
		legacyHead(v0a, 0), legacyLine(0, 40, -1, false, v0a.key, v0a.value),
		legacyHead(v0b, 0), legacyLine(0, 41, -1, false, nil, nil)}})
	v1a := legacy{1, 60, 0, base + 1, []byte("k1"), []byte("v1")}
	vecs = append(vecs, vec{"v1", encodeLegacy(v1a), false, []string{
		legacyHead(v1a, 0), legacyLine(1, 60, base+1, false, v1a.key, v1a.value)}})
	// a v1 gzip wrapper: inner offsets 0..2 relative, the wrapper's offset is the last
	var inner []byte
	for i := 0; i < 3; i++ {
		inner = append(inner, encodeLegacy(legacy{1, int64(i), 0, base + int64(i), []byte{byte('a' + i)}, []byte(fmt.Sprintf("inner-%d", i))})...)
	}
	w1 := legacy{1, 52, 1, base + 9, nil, gz(inner)}
	exp := []string{legacyHead(w1, 1)}
	for i := 0; i < 3; i++ {
		exp = append(exp, legacyLine(1, 50+int64(i), base+int64(i), false, []byte{byte('a' + i)}, []byte(fmt.Sprintf("inner-%d", i))))
	}
	vecs = append(vecs, vec{"v1_gzip", encodeLegacy(w1), false, exp})
	// v0: inner offsets are absolute
	inner = nil
	for i := 0; i < 2; i++ {
		inner = append(inner, encodeLegacy(legacy{0, 7 + int64(i), 0, -1, nil, []byte(fmt.Sprintf("old-%d", i))})...)
	}
	w0 := legacy{0, 8, 1, -1, nil, gz(inner)}
	vecs = append(vecs, vec{"v0_gzip", encodeLegacy(w0), false, []string{legacyHead(w0, 1),
		legacyLine(0, 7, -1, false, nil, []byte("old-0")), legacyLine(0, 8, -1, false, nil, []byte("old-1"))}})

	// ---- varints ----
	type vv struct {
		kind string
		v    int64
		data []byte
	}
	var vvs []vv
	for _, v := range []int64{0, 1, -1, 63, -64, 64, -65, 127, 128, 300, -300, 8191, -8192, 1 << 20, math.MaxInt32, math.MinInt32} {
		vvs = append(vvs, vv{"varint", v, binary.AppendVarint(nil, v)})
	}
	for _, v := range []int64{0, -1, 1 << 40, -(1 << 40), math.MaxInt64, math.MinInt64, 1700000000000} {
		vvs = append(vvs, vv{"varlong", v, binary.AppendVarint(nil, v)})
	}
	for _, v := range []int64{0, 1, 127, 128, 16383, 16384, 2097151, 2097152, math.MaxUint32} {
		vvs = append(vvs, vv{"uvarint", v, binary.AppendUvarint(nil, uint64(v))})
	}

	var src strings.Builder
	src.WriteString("// Generated by tests/wire/gen (go run .); do not edit.\n\n")
	src.WriteString("fn batch_vectors() -> [BatchVec] {\n    let out: [BatchVec] = [];\n")
	var want strings.Builder
	for _, v := range vecs {
		fmt.Fprintf(&src, "    push(out, BatchVec { name: %q, hex: %q, reencode: %v });\n", v.name, hex.EncodeToString(v.data), v.reencode)
		fmt.Fprintf(&want, "batch %s\n", v.name)
		for _, l := range v.expect {
			want.WriteString(l + "\n")
		}
		if v.reencode {
			want.WriteString("  reencode same\n")
		}
	}
	src.WriteString("    return out;\n}\n\nfn varint_vectors() -> [VarVec] {\n    let out: [VarVec] = [];\n")
	for _, v := range vvs {
		lit := fmt.Sprint(v.v)
		if v.v == math.MinInt64 {
			lit = "-9223372036854775807 - 1" // the literal 9223372036854775808 is a u64
		}
		fmt.Fprintf(&src, "    push(out, VarVec { kind: %q, value: %s, hex: %q });\n", v.kind, lit, hex.EncodeToString(v.data))
		fmt.Fprintf(&want, "%s %d ok\n", v.kind, v.v)
	}
	src.WriteString("    return out;\n}\n")
	if err := os.WriteFile("../vectors.sl", []byte(src.String()), 0o644); err != nil {
		panic(err)
	}
	if err := os.WriteFile("../expected.txt", []byte(want.String()), 0o644); err != nil {
		panic(err)
	}
}
