// The Go side of tests/live, with franz-go:
//
//	go run . produce BROKERS TOPIC N   writes N keyed records (zstd, a header)
//	go run . consume BROKERS TOPIC N   reads N records the slang client wrote, and
//	                                   checks values, headers and that each key's
//	                                   partition is Java's murmur2 choice
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Java's Utils.murmur2, written out here to check the slang client against.
func murmur2(data []byte) int32 {
	const m = 0x5bd1e995
	length := len(data)
	h := uint32(0x9747b28c) ^ uint32(length)
	i := 0
	for ; i+4 <= length; i += 4 {
		k := uint32(data[i]) | uint32(data[i+1])<<8 | uint32(data[i+2])<<16 | uint32(data[i+3])<<24
		k *= m
		k ^= k >> 24
		k *= m
		h *= m
		h ^= k
	}
	switch length - i {
	case 3:
		h ^= uint32(data[i+2]) << 16
		fallthrough
	case 2:
		h ^= uint32(data[i+1]) << 8
		fallthrough
	case 1:
		h ^= uint32(data[i])
		h *= m
	}
	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return int32(h)
}

func partitionFor(key []byte, n int) int32 {
	return int32((uint32(murmur2(key)) & 0x7fffffff) % uint32(n))
}

func main() {
	if len(os.Args) != 5 {
		fmt.Fprintln(os.Stderr, "usage: peer produce|consume BROKERS TOPIC N")
		os.Exit(2)
	}
	mode, brokers, topic := os.Args[1], strings.Split(os.Args[2], ","), os.Args[3]
	n, _ := strconv.Atoi(os.Args[4])
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	switch mode {
	case "produce":
		cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ProducerBatchCompression(kgo.ZstdCompression()),
			kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)))
		if err != nil {
			panic(err)
		}
		defer cl.Close()
		for i := 0; i < n; i++ {
			r := &kgo.Record{Topic: topic, Key: []byte(fmt.Sprintf("k%d", i%17)), Value: []byte(fmt.Sprintf("peer-%d", i)),
				Headers: []kgo.RecordHeader{{Key: "from", Value: []byte("franz-go")}}}
			cl.Produce(ctx, r, func(_ *kgo.Record, err error) {
				if err != nil {
					panic(err)
				}
			})
		}
		if err := cl.Flush(ctx); err != nil {
			panic(err)
		}
		fmt.Printf("peer wrote %d records\n", n)
	case "consume":
		cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
		if err != nil {
			panic(err)
		}
		defer cl.Close()
		got, placement, values, headers := 0, true, true, true
		for got < n && ctx.Err() == nil {
			fs := cl.PollFetches(ctx)
			fs.EachRecord(func(r *kgo.Record) {
				got++
				if !strings.HasPrefix(string(r.Value), "slang-") {
					values = false
				}
				if len(r.Headers) != 1 || r.Headers[0].Key != "from" || string(r.Headers[0].Value) != "slang" {
					headers = false
				}
				// tests/live creates the topic with 3 partitions
				if r.Partition != partitionFor(r.Key, 3) {
					placement = false
				}
			})
		}
		fmt.Printf("peer read %d records: values %v, headers %v, keyed placement matches murmur2 %v\n", got, values,
			headers, placement)
	}
}
