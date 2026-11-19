// Codec vectors for the slang codecs, written by the Go libraries Kafka
// clients use: klauspost/compress (zstd), pierrec/lz4 (lz4 frames),
// golang/snappy (raw blocks; xerial framing is built here as Java's
// SnappyOutputStream writes it) and compress/gzip.
//
//	go run .              writes ../vectors.bin and ../expected.txt
//	go run . check FILE   decompresses what tests/codec wrote with the
//	                      slang encoders, and checks it against the inputs
//
// vectors.bin.gz (gzip) holds the inputs once: a u16 count, then for each
// a name (u16 length + bytes), length (u32) and the bytes; then vectors:
// name, codec (u8), input index (u16), compressed length (u32), bytes.
// check reads the layout tests/codec writes: name, codec, input length
// (u32), input, compressed length (u32), compressed.
package main

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

type input struct {
	name string
	data []byte
}

func inputs() []input {
	rng := rand.New(rand.NewSource(42))
	random := make([]byte, 20000)
	rng.Read(random)
	var js strings.Builder
	for i := 0; js.Len() < 150000; i++ {
		fmt.Fprintf(&js, `{"id":%d,"user":"user-%d","event":"order.created","amount":%d.%02d,"tags":["a","b%d"]}`+"\n",
			i, i%97, i*7%1000, i%100, i%13)
	}
	var text strings.Builder
	words := []string{"kafka", "broker", "partition", "offset", "consumer", "group", "leader", "replica", "log", "segment"}
	for text.Len() < 200000 {
		text.WriteString(words[rng.Intn(len(words))])
		if rng.Intn(9) == 0 {
			text.WriteString(".\n")
		} else {
			text.WriteString(" ")
		}
	}
	mixed := make([]byte, 0, 60000)
	for len(mixed) < 60000 {
		n := rng.Intn(300)
		if rng.Intn(2) == 0 {
			mixed = append(mixed, bytes.Repeat([]byte{byte(rng.Intn(256))}, n)...)
		} else {
			b := make([]byte, n)
			rng.Read(b)
			mixed = append(mixed, b...)
		}
	}
	return []input{
		{"empty", []byte{}},
		{"one", []byte("x")},
		{"short", []byte("hello, kafka hello, kafka hello")},
		{"zeros", make([]byte, 300000)},
		{"random", random},
		{"json", []byte(js.String())},
		{"text", []byte(text.String())},
		{"mixed", mixed},
	}
}

type vec struct {
	name  string
	codec int
	input int
	in    []byte
	out   []byte
}

func gz(b []byte) []byte {
	var z bytes.Buffer
	w := gzip.NewWriter(&z)
	w.Write(b)
	w.Close()
	return z.Bytes()
}

func gzBest(b []byte) []byte {
	var z bytes.Buffer
	w, _ := gzip.NewWriterLevel(&z, gzip.BestCompression)
	w.Write(b)
	w.Close()
	return z.Bytes()
}

func xerial(b []byte) []byte {
	out := []byte{0x82, 'S', 'N', 'A', 'P', 'P', 'Y', 0, 0, 0, 0, 1, 0, 0, 0, 1}
	for len(b) > 0 {
		n := 32768
		if n > len(b) {
			n = len(b)
		}
		blk := snappy.Encode(nil, b[:n])
		out = binary.BigEndian.AppendUint32(out, uint32(len(blk)))
		out = append(out, blk...)
		b = b[n:]
	}
	return out
}

func lz4w(b []byte, opts ...lz4.Option) []byte {
	var z bytes.Buffer
	w := lz4.NewWriter(&z)
	if err := w.Apply(opts...); err != nil {
		panic(err)
	}
	w.Write(b)
	w.Close()
	return z.Bytes()
}

func zstdw(b []byte, opts ...zstd.EOption) []byte {
	opts = append(opts, zstd.WithEncoderConcurrency(1))
	enc, err := zstd.NewWriter(nil, opts...)
	if err != nil {
		panic(err)
	}
	return enc.EncodeAll(b, nil)
}

func zstdStream(b []byte, opts ...zstd.EOption) []byte {
	var z bytes.Buffer
	opts = append(opts, zstd.WithEncoderConcurrency(1))
	enc, err := zstd.NewWriter(&z, opts...)
	if err != nil {
		panic(err)
	}
	enc.Write(b)
	enc.Close()
	return z.Bytes()
}

func vectors() []vec {
	var vs []vec
	for idx, in := range inputs() {
		add := func(variant string, codec int, out []byte) {
			vs = append(vs, vec{in.name + "/" + variant, codec, idx, in.data, out})
		}
		add("gzip", 1, gz(in.data))
		add("snappy-raw", 2, snappy.Encode(nil, in.data))
		add("snappy-xerial", 2, xerial(in.data))
		add("lz4-default", 3, lz4w(in.data))
		add("lz4-64k-nochecksum", 3, lz4w(in.data, lz4.BlockSizeOption(lz4.Block64Kb), lz4.ChecksumOption(false)))
		add("lz4-blockchecksum-size", 3, lz4w(in.data, lz4.BlockChecksumOption(true), lz4.SizeOption(uint64(len(in.data)))))
		add("lz4-hc", 3, lz4w(in.data, lz4.CompressionLevelOption(lz4.Level9)))
		add("lz4-4m", 3, lz4w(in.data, lz4.BlockSizeOption(lz4.Block4Mb)))
		add("zstd-fastest", 4, zstdw(in.data, zstd.WithEncoderLevel(zstd.SpeedFastest)))
		add("zstd-default-crc", 4, zstdw(in.data, zstd.WithEncoderCRC(true)))
		add("zstd-better", 4, zstdw(in.data, zstd.WithEncoderLevel(zstd.SpeedBetterCompression)))
		add("zstd-best", 4, zstdw(in.data, zstd.WithEncoderLevel(zstd.SpeedBestCompression)))
		add("zstd-stream-window", 4, zstdStream(in.data, zstd.WithWindowSize(1<<16), zstd.WithEncoderCRC(false)))
	}
	return vs
}

func digest(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:8])
}

func write() {
	var bin bytes.Buffer
	var want strings.Builder
	ins := inputs()
	binary.Write(&bin, binary.BigEndian, uint16(len(ins)))
	for _, in := range ins {
		binary.Write(&bin, binary.BigEndian, uint16(len(in.name)))
		bin.WriteString(in.name)
		binary.Write(&bin, binary.BigEndian, uint32(len(in.data)))
		bin.Write(in.data)
	}
	for _, v := range vectors() {
		binary.Write(&bin, binary.BigEndian, uint16(len(v.name)))
		bin.WriteString(v.name)
		bin.WriteByte(byte(v.codec))
		binary.Write(&bin, binary.BigEndian, uint16(v.input))
		binary.Write(&bin, binary.BigEndian, uint32(len(v.out)))
		bin.Write(v.out)
		fmt.Fprintf(&want, "%s %d %s\n", v.name, len(v.in), digest(v.in))
	}
	// what tests/codec prints after the vectors: its own encoders round
	// trip every input, survive every mutation, and honour the size limit
	for _, in := range ins {
		for _, c := range []string{"gzip", "snappy", "lz4", "zstd"} {
			fmt.Fprintf(&want, "roundtrip %s %s ok\n", in.name, c)
		}
	}
	fmt.Fprintf(&want, "hostile %d mutations, no crash\n", len(vectors())*6)
	fmt.Fprintf(&want, "limits ok\n")
	must(os.WriteFile("../vectors.bin.gz", gzBest(bin.Bytes()), 0o644))
	must(os.WriteFile("../expected.txt", []byte(want.String()), 0o644))
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

// check reads records of name, codec, input, compressed (the vectors.bin
// layout) written by the slang encoders, and decompresses each with Go.
func check(path string) {
	data, err := os.ReadFile(path)
	must(err)
	bad := 0
	n := 0
	for len(data) > 0 {
		nl := int(binary.BigEndian.Uint16(data))
		name := string(data[2 : 2+nl])
		data = data[2+nl:]
		codec := data[0]
		il := int(binary.BigEndian.Uint32(data[1:]))
		in := data[5 : 5+il]
		data = data[5+il:]
		cl := int(binary.BigEndian.Uint32(data))
		comp := data[4 : 4+cl]
		data = data[4+cl:]
		var got []byte
		switch codec {
		case 1:
			r, e := gzip.NewReader(bytes.NewReader(comp))
			if e == nil {
				got, e = io.ReadAll(r)
			}
			err = e
		case 2:
			got, err = unxerial(comp)
		case 3:
			got, err = io.ReadAll(lz4.NewReader(bytes.NewReader(comp)))
		case 4:
			d, _ := zstd.NewReader(nil)
			got, err = d.DecodeAll(comp, nil)
		}
		n++
		if err != nil || !bytes.Equal(got, in) {
			bad++
			fmt.Printf("FAIL %s: %v (got %d bytes, want %d)\n", name, err, len(got), len(in))
		}
	}
	fmt.Printf("checked %d slang-compressed inputs with Go: %d failed\n", n, bad)
	if bad > 0 {
		os.Exit(1)
	}
}

func unxerial(b []byte) ([]byte, error) {
	if len(b) < 16 || !bytes.Equal(b[:8], []byte{0x82, 'S', 'N', 'A', 'P', 'P', 'Y', 0}) {
		return snappy.Decode(nil, b)
	}
	var out []byte
	b = b[16:]
	for len(b) > 0 {
		n := int(binary.BigEndian.Uint32(b))
		d, err := snappy.Decode(nil, b[4:4+n])
		if err != nil {
			return nil, err
		}
		out = append(out, d...)
		b = b[4+n:]
	}
	return out, nil
}

func main() {
	if len(os.Args) > 2 && os.Args[1] == "check" {
		check(os.Args[2])
		return
	}
	write()
}
