// The franz-go side of bench/run.sh: the same three modes, settings and
// output line as bench/slang (acks=all, idempotent, 5 ms linger, 1 MiB
// batches, sticky partitioning for unkeyed records).
package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func envInt(name string, d int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil {
		return v
	}
	return d
}

func env(name, d string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return d
}

func main() {
	brokers := strings.Split(env("BENCH_BROKERS", "localhost:19092"), ",")
	mode, topic := env("BENCH_MODE", "produce"), env("BENCH_TOPIC", "bench")
	n, size, codec := envInt("BENCH_N", 1000000), envInt("BENCH_SIZE", 100), envInt("BENCH_CODEC", 0)
	ctx := context.Background()
	codecs := []kgo.CompressionCodec{kgo.NoCompression(), kgo.GzipCompression(), kgo.SnappyCompression(),
		kgo.Lz4Compression(), kgo.ZstdCompression()}
	switch mode {
	case "produce":
		cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ProducerLinger(5*time.Millisecond),
			kgo.ProducerBatchMaxBytes(1048576), kgo.ProducerBatchCompression(codecs[codec]))
		if err != nil {
			panic(err)
		}
		value := make([]byte, size)
		for i := range value {
			value[i] = byte('a' + i%26)
		}
		t0 := time.Now()
		for i := 0; i < n; i++ {
			cl.Produce(ctx, &kgo.Record{Topic: topic, Value: value}, func(_ *kgo.Record, err error) {
				if err != nil {
					panic(err)
				}
			})
		}
		if err := cl.Flush(ctx); err != nil {
			panic(err)
		}
		d := time.Since(t0)
		fmt.Printf("go produce records=%d ms=%d rec/s=%d MB/s=%d\n", n, d.Milliseconds(), int64(float64(n)/d.Seconds()),
			int64(float64(n*size)/d.Seconds()/1e6))
		cl.Close()
	case "consume":
		cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
		if err != nil {
			panic(err)
		}
		got, bytes := 0, 0
		t0 := time.Now()
		for got < n {
			c, cancel := context.WithTimeout(ctx, 5*time.Second)
			fs := cl.PollRecords(c, 500)
			cancel()
			if fs.NumRecords() == 0 {
				break
			}
			fs.EachRecord(func(r *kgo.Record) {
				got++
				bytes += len(r.Value)
			})
		}
		d := time.Since(t0)
		fmt.Printf("go consume records=%d ms=%d rec/s=%d MB/s=%d\n", got, d.Milliseconds(), int64(float64(got)/d.Seconds()),
			int64(float64(bytes)/d.Seconds()/1e6))
		cl.Close()
	default:
		cons, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {0: kgo.NewOffset().AtEnd()}}), kgo.FetchMaxWait(100*time.Millisecond))
		if err != nil {
			panic(err)
		}
		prod, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ProducerLinger(0),
			kgo.RecordPartitioner(kgo.ManualPartitioner()))
		if err != nil {
			panic(err)
		}
		c, cancel := context.WithTimeout(ctx, time.Second)
		cons.PollFetches(c)
		cancel()
		var lat []int
		for i := 0; i < n; i++ {
			sent := time.Now()
			if err := prod.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: 0, Value: []byte("x")}).FirstErr(); err != nil {
				panic(err)
			}
			for {
				c, cancel := context.WithTimeout(ctx, 5*time.Second)
				fs := cons.PollFetches(c)
				cancel()
				if fs.NumRecords() > 0 {
					lat = append(lat, int(time.Since(sent).Microseconds()))
					break
				}
			}
		}
		sort.Ints(lat)
		fmt.Printf("go latency records=%d p50_us=%d p99_us=%d\n", len(lat), lat[len(lat)/2], lat[len(lat)*99/100])
		prod.Close()
		cons.Close()
	}
}
