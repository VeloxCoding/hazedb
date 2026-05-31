package spike_test

import (
	"database/sql"
	"encoding/binary"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/VeloxCoding/hazedb/spike"
)

// ====================================================================
//  The thesis test: ordered tail scan
//
//  Workload: 1000 threads × 100 messages each = 100k messages.
//  Query:    "Last 20 messages of thread T, descending by seq"
//
//  Compare go_sqldb's per-thread sorted index against SQLite-in-memory
//  with an index on (thread_id, seq). This is the RFC's canonical
//  Phase-2 query and the operation where ordered indexes are supposed
//  to differentiate.
// ====================================================================

const (
	tsThreads   = 1_000
	tsPerThread = 100
	tsTotal     = tsThreads * tsPerThread
)

func preGenMessages() ([]spike.Message, [][16]byte) {
	msgs := make([]spike.Message, 0, tsTotal)
	threadIDs := make([][16]byte, tsThreads)
	now := time.Now().UnixMilli()
	for t := 0; t < tsThreads; t++ {
		var tid [16]byte
		binary.LittleEndian.PutUint64(tid[0:8], uint64(t)*2654435761)
		binary.LittleEndian.PutUint64(tid[8:16], uint64(t)*1442695040888963407)
		threadIDs[t] = tid
		for s := 0; s < tsPerThread; s++ {
			var id spike.UUIDv7
			ts := now + int64(t*tsPerThread+s)
			id[0] = byte(ts >> 40)
			id[1] = byte(ts >> 32)
			id[2] = byte(ts >> 24)
			id[3] = byte(ts >> 16)
			id[4] = byte(ts >> 8)
			id[5] = byte(ts)
			binary.LittleEndian.PutUint64(id[6:14], uint64(t*tsPerThread+s))
			binary.LittleEndian.PutUint16(id[14:16], uint16(s))
			id[6] = (id[6] & 0x0F) | 0x70
			id[8] = (id[8] & 0x3F) | 0x80
			msgs = append(msgs, spike.Message{
				ID:       id,
				ThreadID: tid,
				Seq:      int64(s),
				Body:     "msg " + intToStr(t) + "-" + intToStr(s),
			})
		}
	}
	return msgs, threadIDs
}

func setupMessagesDB(b *testing.B) (*spike.MessagesDB, [][16]byte) {
	b.Helper()
	db := spike.OpenMessagesDB(tsTotal)
	msgs, tids := preGenMessages()
	for i := range msgs {
		if err := db.Insert(msgs[i]); err != nil {
			b.Fatal(err)
		}
	}
	return db, tids
}

func setupSQLiteMessages(b *testing.B) (*sql.DB, [][16]byte) {
	b.Helper()
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		b.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`
		PRAGMA journal_mode=MEMORY;
		PRAGMA synchronous=OFF;
		CREATE TABLE messages (
			id BLOB PRIMARY KEY,
			thread_id BLOB NOT NULL,
			seq INTEGER NOT NULL,
			body TEXT NOT NULL
		);
		CREATE INDEX msg_idx ON messages(thread_id, seq);
	`); err != nil {
		b.Fatal(err)
	}
	msgs, tids := preGenMessages()
	ins, _ := db.Prepare("INSERT INTO messages(id, thread_id, seq, body) VALUES(?,?,?,?)")
	for i := range msgs {
		ins.Exec(msgs[i].ID[:], msgs[i].ThreadID[:], msgs[i].Seq, msgs[i].Body)
	}
	ins.Close()
	return db, tids
}

// --- go_sqldb: tail scan ---

func Benchmark_Messages_LastN_go_sqldb(b *testing.B) {
	db, tids := setupMessagesDB(b)
	defer db.Close()
	buf := make([]spike.Message, 0, 30)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tid := tids[i%len(tids)]
		buf = db.LastN(tid, 20, buf[:0])
		if len(buf) != 20 {
			b.Fatalf("got %d, want 20", len(buf))
		}
	}
}

func Benchmark_Messages_LastN_go_sqldb_Parallel(b *testing.B) {
	db, tids := setupMessagesDB(b)
	defer db.Close()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		buf := make([]spike.Message, 0, 30)
		i := 0
		for pb.Next() {
			tid := tids[i%len(tids)]
			buf = db.LastN(tid, 20, buf[:0])
			if len(buf) != 20 {
				b.Fatalf("got %d, want 20", len(buf))
			}
			i++
		}
	})
}

// --- SQLite: tail scan with index ---

func Benchmark_Messages_LastN_SQLite(b *testing.B) {
	db, tids := setupSQLiteMessages(b)
	defer db.Close()
	stmt, err := db.Prepare("SELECT id, thread_id, seq, body FROM messages WHERE thread_id = ? ORDER BY seq DESC LIMIT 20")
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()
	b.ReportAllocs()
	b.ResetTimer()
	var id, threadID, body []byte
	var seq int64
	for i := 0; i < b.N; i++ {
		tid := tids[i%len(tids)]
		rows, err := stmt.Query(tid[:])
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for rows.Next() {
			rows.Scan(&id, &threadID, &seq, &body)
			count++
		}
		rows.Close()
		if count != 20 {
			b.Fatalf("got %d, want 20", count)
		}
	}
}

// --- Bonus: point lookup by PK ---

func Benchmark_Messages_GetByID_go_sqldb(b *testing.B) {
	// Build msgs once and reuse, since preGenMessages uses time.Now()
	// and would produce different UUIDs in two separate calls.
	db := spike.OpenMessagesDB(tsTotal)
	defer db.Close()
	msgs, _ := preGenMessages()
	for i := range msgs {
		if err := db.Insert(msgs[i]); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m := msgs[i%len(msgs)]
		_, ok := db.GetByID(m.ID, m.ThreadID)
		if !ok {
			b.Fatal("missing")
		}
	}
}

func Benchmark_Messages_GetByID_SQLite(b *testing.B) {
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.Exec(`
		PRAGMA journal_mode=MEMORY; PRAGMA synchronous=OFF;
		CREATE TABLE messages (id BLOB PRIMARY KEY, thread_id BLOB NOT NULL, seq INTEGER NOT NULL, body TEXT NOT NULL);
		CREATE INDEX msg_idx ON messages(thread_id, seq);
	`)
	msgs, _ := preGenMessages()
	ins, _ := db.Prepare("INSERT INTO messages(id, thread_id, seq, body) VALUES(?,?,?,?)")
	for i := range msgs {
		ins.Exec(msgs[i].ID[:], msgs[i].ThreadID[:], msgs[i].Seq, msgs[i].Body)
	}
	ins.Close()
	stmt, err := db.Prepare("SELECT thread_id, seq, body FROM messages WHERE id = ?")
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()
	b.ReportAllocs()
	b.ResetTimer()
	var threadID, body []byte
	var seq int64
	for i := 0; i < b.N; i++ {
		m := msgs[i%len(msgs)]
		if err := stmt.QueryRow(m.ID[:]).Scan(&threadID, &seq, &body); err != nil {
			b.Fatal(err)
		}
	}
}
