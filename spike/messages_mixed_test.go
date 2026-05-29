package spike_test

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/bbolt"

	"github.com/VeloxCoding/hazedb/spike"
)

// ====================================================================
//  WAL overhead — validates the ~150 ns claim
// ====================================================================

func Benchmark_Messages_Insert_NoWAL(b *testing.B) {
	db := spike.OpenMessagesDB(b.N)
	defer db.Close()
	msgs := buildLinearMessages(b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Insert(msgs[i]); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Messages_Insert_WAL(b *testing.B) {
	path := filepath.Join(b.TempDir(), "msg.wal")
	db, err := spike.OpenMessagesDBWAL(path, b.N)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	msgs := buildLinearMessages(b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Insert(msgs[i]); err != nil {
			b.Fatal(err)
		}
	}
}

// buildLinearMessages — N messages spread across tsThreads threads,
// monotonically increasing seq within each thread. Same pattern as
// preGenMessages but parameterised on count.
func buildLinearMessages(n int) []spike.Message {
	out := make([]spike.Message, n)
	now := time.Now().UnixMilli()
	threadIDs := make([][16]byte, tsThreads)
	for t := 0; t < tsThreads; t++ {
		var tid [16]byte
		binary.LittleEndian.PutUint64(tid[0:8], uint64(t)*2654435761)
		binary.LittleEndian.PutUint64(tid[8:16], uint64(t)*1442695040888963407)
		threadIDs[t] = tid
	}
	seqByThread := make([]int64, tsThreads)
	for i := 0; i < n; i++ {
		t := i % tsThreads
		var id spike.UUIDv7
		ts := now + int64(i)
		id[0] = byte(ts >> 40)
		id[1] = byte(ts >> 32)
		id[2] = byte(ts >> 24)
		id[3] = byte(ts >> 16)
		id[4] = byte(ts >> 8)
		id[5] = byte(ts)
		binary.LittleEndian.PutUint64(id[6:14], uint64(i)*2654435761)
		binary.LittleEndian.PutUint16(id[14:16], uint16(i))
		id[6] = (id[6] & 0x0F) | 0x70
		id[8] = (id[8] & 0x3F) | 0x80
		out[i] = spike.Message{
			ID:       id,
			ThreadID: threadIDs[t],
			Seq:      seqByThread[t],
			Body:     "msg",
		}
		seqByThread[t]++
	}
	return out
}

// ====================================================================
//  BoltDB tail-scan — honest pure-Go baseline
//
//  Bolt is a B+tree with cursors and reverse iteration. The fair
//  comparison: "how fast can a typical Go embedded DB do this query?"
// ====================================================================

func setupBoltMessages(b *testing.B) (*bbolt.DB, [][16]byte) {
	b.Helper()
	path := filepath.Join(b.TempDir(), "bolt.db")
	db, err := bbolt.Open(path, 0644, nil)
	if err != nil {
		b.Fatal(err)
	}
	_, tids := preGenMessages()
	// Bolt strategy: one bucket per thread, keys = big-endian seq for
	// natural sort order. This is how you'd index "messages by thread,
	// ordered by seq" in BoltDB.
	msgs, _ := preGenMessages()
	db.Update(func(tx *bbolt.Tx) error {
		for t := 0; t < tsThreads; t++ {
			tid := tids[t]
			bkt, _ := tx.CreateBucket(append([]byte("t:"), tid[:]...))
			for s := 0; s < tsPerThread; s++ {
				m := msgs[t*tsPerThread+s]
				var seqKey [8]byte
				binary.BigEndian.PutUint64(seqKey[:], uint64(m.Seq))
				bkt.Put(seqKey[:], []byte(m.Body))
			}
		}
		return nil
	})
	return db, tids
}

func Benchmark_Messages_LastN_Bolt(b *testing.B) {
	db, tids := setupBoltMessages(b)
	defer db.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tid := tids[i%len(tids)]
		err := db.View(func(tx *bbolt.Tx) error {
			bkt := tx.Bucket(append([]byte("t:"), tid[:]...))
			c := bkt.Cursor()
			count := 0
			for k, _ := c.Last(); k != nil && count < 20; k, _ = c.Prev() {
				count++
			}
			if count != 20 {
				return fmt.Errorf("got %d", count)
			}
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Messages_LastN_Bolt_Parallel(b *testing.B) {
	db, tids := setupBoltMessages(b)
	defer db.Close()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			tid := tids[i%len(tids)]
			db.View(func(tx *bbolt.Tx) error {
				bkt := tx.Bucket(append([]byte("t:"), tid[:]...))
				c := bkt.Cursor()
				count := 0
				for k, _ := c.Last(); k != nil && count < 20; k, _ = c.Prev() {
					count++
				}
				return nil
			})
			i++
		}
	})
}

// ====================================================================
//  Mixed workload — concurrent insert + tail-scan, p50/p99
//
//  N producers continuously insert into random threads.
//  M consumers continuously tail-scan random threads.
//  Run for fixed wall-clock duration; collect per-op latency samples;
//  report p50, p99, p999 separately.
// ====================================================================

// percentile returns the value at fraction p (e.g. 0.99) of a sorted slice.
func percentile(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)) * p)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func runMixedBench(b *testing.B, useWAL bool) {
	const (
		warmThreads     = 1_000
		warmPerThread   = 100
		insertWorkers   = 8
		scanWorkers     = 24
		duration        = 5 * time.Second
		sampleBatchSize = 100
	)

	var db *spike.MessagesDB
	var err error
	if useWAL {
		path := filepath.Join(b.TempDir(), "msg.wal")
		db, err = spike.OpenMessagesDBWAL(path, warmThreads*warmPerThread*4)
		if err != nil {
			b.Fatal(err)
		}
	} else {
		db = spike.OpenMessagesDB(warmThreads * warmPerThread * 4)
	}
	defer db.Close()

	// Warm-up
	warm := buildLinearMessages(warmThreads * warmPerThread)
	for _, m := range warm {
		db.Insert(m)
	}

	// Pre-build tids list
	tids := make([][16]byte, warmThreads)
	for t := 0; t < warmThreads; t++ {
		binary.LittleEndian.PutUint64(tids[t][0:8], uint64(t)*2654435761)
		binary.LittleEndian.PutUint64(tids[t][8:16], uint64(t)*1442695040888963407)
	}

	stop := time.Now().Add(duration)

	var (
		wg            sync.WaitGroup
		insertSamples [][]int64
		scanSamples   [][]int64
		insertOps     atomic.Int64
		scanOps       atomic.Int64
		nextSeq       = make([]int64, warmThreads)
	)
	insertSamples = make([][]int64, insertWorkers)
	scanSamples = make([][]int64, scanWorkers)
	for t := 0; t < warmThreads; t++ {
		nextSeq[t] = int64(warmPerThread)
	}
	var seqMu sync.Mutex // protects nextSeq[] increments

	// Insert workers
	for wid := 0; wid < insertWorkers; wid++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			samples := make([]int64, 0, 1<<16)
			rng := uint32(wid*1103515245 + 12345)
			for time.Now().Before(stop) {
				start := time.Now()
				for k := 0; k < sampleBatchSize; k++ {
					rng = rng*1664525 + 1013904223
					t := int(rng % warmThreads)
					seqMu.Lock()
					seq := nextSeq[t]
					nextSeq[t]++
					seqMu.Unlock()
					var id spike.UUIDv7
					binary.LittleEndian.PutUint64(id[0:8], uint64(time.Now().UnixNano()))
					binary.LittleEndian.PutUint64(id[8:16], uint64(seq)+uint64(t)<<32)
					id[6] = (id[6] & 0x0F) | 0x70
					id[8] = (id[8] & 0x3F) | 0x80
					db.Insert(spike.Message{
						ID:       id,
						ThreadID: tids[t],
						Seq:      seq,
						Body:     "m",
					})
				}
				batchNs := time.Since(start).Nanoseconds()
				samples = append(samples, batchNs/int64(sampleBatchSize))
				insertOps.Add(int64(sampleBatchSize))
			}
			insertSamples[wid] = samples
		}(wid)
	}

	// Scan workers
	for wid := 0; wid < scanWorkers; wid++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			samples := make([]int64, 0, 1<<18)
			buf := make([]spike.Message, 0, 30)
			rng := uint32(wid*1664525 + 1013904223)
			for time.Now().Before(stop) {
				start := time.Now()
				for k := 0; k < sampleBatchSize; k++ {
					rng = rng*1103515245 + 12345
					t := int(rng % warmThreads)
					buf = db.LastN(tids[t], 20, buf[:0])
					_ = buf
				}
				batchNs := time.Since(start).Nanoseconds()
				samples = append(samples, batchNs/int64(sampleBatchSize))
				scanOps.Add(int64(sampleBatchSize))
			}
			scanSamples[wid] = samples
		}(wid)
	}

	wg.Wait()

	// Aggregate samples
	var allInsert, allScan []int64
	for _, s := range insertSamples {
		allInsert = append(allInsert, s...)
	}
	for _, s := range scanSamples {
		allScan = append(allScan, s...)
	}
	sort.Slice(allInsert, func(i, j int) bool { return allInsert[i] < allInsert[j] })
	sort.Slice(allScan, func(i, j int) bool { return allScan[i] < allScan[j] })

	mode := "memory"
	if useWAL {
		mode = "WAL+bufio"
	}
	b.Logf("=== Mixed workload: %d insert workers + %d scan workers, %s, mode=%s ===",
		insertWorkers, scanWorkers, duration, mode)
	b.Logf("INSERT: %d ops, p50=%d ns, p99=%d ns, p999=%d ns, max=%d ns",
		insertOps.Load(),
		percentile(allInsert, 0.50),
		percentile(allInsert, 0.99),
		percentile(allInsert, 0.999),
		allInsert[len(allInsert)-1])
	b.Logf("SCAN:   %d ops, p50=%d ns, p99=%d ns, p999=%d ns, max=%d ns",
		scanOps.Load(),
		percentile(allScan, 0.50),
		percentile(allScan, 0.99),
		percentile(allScan, 0.999),
		allScan[len(allScan)-1])
	b.Logf("Aggregate throughput: %.2fM inserts/sec, %.2fM scans/sec",
		float64(insertOps.Load())/duration.Seconds()/1e6,
		float64(scanOps.Load())/duration.Seconds()/1e6)
	// Force the benchmark framework to register something
	b.SetBytes(int64(insertOps.Load() + scanOps.Load()))
}

func Benchmark_Messages_Mixed_NoWAL(b *testing.B) {
	for n := 0; n < b.N; n++ {
		runMixedBench(b, false)
	}
}

func Benchmark_Messages_Mixed_WAL(b *testing.B) {
	for n := 0; n < b.N; n++ {
		runMixedBench(b, true)
	}
}

// ====================================================================
//  Skewed-thread workload — 90% of ops to 10% of threads
//
//  Real chat-app pattern: a handful of active threads dominate. Tests
//  whether 128-shard sharding holds up when the hot threads cluster
//  on a subset of shards. If p99 collapses, sharding needs rethink.
// ====================================================================

func runSkewedBench(b *testing.B, useWAL bool, hotShare float64) {
	const (
		totalThreads    = 1_000
		hotThreads      = 100 // 10% of threads
		warmPerThread   = 100
		insertWorkers   = 8
		scanWorkers     = 24
		duration        = 5 * time.Second
		sampleBatchSize = 100
	)

	var db *spike.MessagesDB
	var err error
	if useWAL {
		path := filepath.Join(b.TempDir(), "msg.wal")
		db, err = spike.OpenMessagesDBWAL(path, totalThreads*warmPerThread*4)
		if err != nil {
			b.Fatal(err)
		}
	} else {
		db = spike.OpenMessagesDB(totalThreads * warmPerThread * 4)
	}
	defer db.Close()

	warm := buildLinearMessages(totalThreads * warmPerThread)
	for _, m := range warm {
		db.Insert(m)
	}

	tids := make([][16]byte, totalThreads)
	for t := 0; t < totalThreads; t++ {
		binary.LittleEndian.PutUint64(tids[t][0:8], uint64(t)*2654435761)
		binary.LittleEndian.PutUint64(tids[t][8:16], uint64(t)*1442695040888963407)
	}

	stop := time.Now().Add(duration)

	var (
		wg            sync.WaitGroup
		insertSamples = make([][]int64, insertWorkers)
		scanSamples   = make([][]int64, scanWorkers)
		insertOps     atomic.Int64
		scanOps       atomic.Int64
		nextSeq       = make([]int64, totalThreads)
		hotHits       atomic.Int64
		coldHits      atomic.Int64
	)
	for t := 0; t < totalThreads; t++ {
		nextSeq[t] = int64(warmPerThread)
	}
	var seqMu sync.Mutex

	hotCut := uint32(hotShare * float64(^uint32(0)))

	pickThread := func(rng uint32) int {
		if rng < hotCut {
			return int(rng % hotThreads)
		}
		return hotThreads + int(rng%(totalThreads-hotThreads))
	}

	for wid := 0; wid < insertWorkers; wid++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			samples := make([]int64, 0, 1<<16)
			rng := uint32(wid*1103515245 + 12345)
			for time.Now().Before(stop) {
				start := time.Now()
				for k := 0; k < sampleBatchSize; k++ {
					rng = rng*1664525 + 1013904223
					t := pickThread(rng)
					if t < hotThreads {
						hotHits.Add(1)
					} else {
						coldHits.Add(1)
					}
					seqMu.Lock()
					seq := nextSeq[t]
					nextSeq[t]++
					seqMu.Unlock()
					var id spike.UUIDv7
					binary.LittleEndian.PutUint64(id[0:8], uint64(time.Now().UnixNano()))
					binary.LittleEndian.PutUint64(id[8:16], uint64(seq)+uint64(t)<<32)
					id[6] = (id[6] & 0x0F) | 0x70
					id[8] = (id[8] & 0x3F) | 0x80
					db.Insert(spike.Message{
						ID:       id,
						ThreadID: tids[t],
						Seq:      seq,
						Body:     "m",
					})
				}
				batchNs := time.Since(start).Nanoseconds()
				samples = append(samples, batchNs/int64(sampleBatchSize))
				insertOps.Add(int64(sampleBatchSize))
			}
			insertSamples[wid] = samples
		}(wid)
	}

	for wid := 0; wid < scanWorkers; wid++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			samples := make([]int64, 0, 1<<18)
			buf := make([]spike.Message, 0, 30)
			rng := uint32(wid*1664525 + 1013904223)
			for time.Now().Before(stop) {
				start := time.Now()
				for k := 0; k < sampleBatchSize; k++ {
					rng = rng*1103515245 + 12345
					t := pickThread(rng)
					buf = db.LastN(tids[t], 20, buf[:0])
					_ = buf
				}
				batchNs := time.Since(start).Nanoseconds()
				samples = append(samples, batchNs/int64(sampleBatchSize))
				scanOps.Add(int64(sampleBatchSize))
			}
			scanSamples[wid] = samples
		}(wid)
	}

	wg.Wait()

	var allInsert, allScan []int64
	for _, s := range insertSamples {
		allInsert = append(allInsert, s...)
	}
	for _, s := range scanSamples {
		allScan = append(allScan, s...)
	}
	sort.Slice(allInsert, func(i, j int) bool { return allInsert[i] < allInsert[j] })
	sort.Slice(allScan, func(i, j int) bool { return allScan[i] < allScan[j] })

	mode := "memory"
	if useWAL {
		mode = "WAL+bufio"
	}
	b.Logf("=== Skewed: %.0f%% ops to %d/%d threads, %s, mode=%s ===",
		hotShare*100, hotThreads, totalThreads, duration, mode)
	b.Logf("Skew check: hot=%d cold=%d (%.1f%% hot)",
		hotHits.Load(), coldHits.Load(),
		100*float64(hotHits.Load())/float64(hotHits.Load()+coldHits.Load()))
	b.Logf("INSERT: %d ops, p50=%d ns, p99=%d ns, p999=%d ns, max=%d ns",
		insertOps.Load(),
		percentile(allInsert, 0.50),
		percentile(allInsert, 0.99),
		percentile(allInsert, 0.999),
		allInsert[len(allInsert)-1])
	b.Logf("SCAN:   %d ops, p50=%d ns, p99=%d ns, p999=%d ns, max=%d ns",
		scanOps.Load(),
		percentile(allScan, 0.50),
		percentile(allScan, 0.99),
		percentile(allScan, 0.999),
		allScan[len(allScan)-1])
	b.Logf("Aggregate: %.2fM inserts/sec, %.2fM scans/sec",
		float64(insertOps.Load())/duration.Seconds()/1e6,
		float64(scanOps.Load())/duration.Seconds()/1e6)
	b.SetBytes(int64(insertOps.Load() + scanOps.Load()))
}

func Benchmark_Messages_Skewed_NoWAL_90pct(b *testing.B) {
	for n := 0; n < b.N; n++ {
		runSkewedBench(b, false, 0.9)
	}
}

func Benchmark_Messages_Skewed_WAL_90pct(b *testing.B) {
	for n := 0; n < b.N; n++ {
		runSkewedBench(b, true, 0.9)
	}
}

func Benchmark_Messages_Skewed_WAL_99pct(b *testing.B) {
	for n := 0; n < b.N; n++ {
		runSkewedBench(b, true, 0.99)
	}
}

// Suppress unused-import warning if the file is partially edited.
var _ = runtime.NumCPU
