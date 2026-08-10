package coherence

import (
	"context"
	"sync"
	"testing"
	"time"
)

// These mirror spec/coherence.qnt's invRecallSafety and the
// RequestPosixMutate / AckRecall / CompletePosixMutate actions: a posix
// mutation may only proceed once every current lease holder has acked or
// blown its DRECALL deadline.

func TestRecallBlocksUntilHolderAcks(t *testing.T) {
	m, _ := newTestManager()
	const obj = "inode:1"

	m.Grant("reader", obj)
	var recallID uint64
	gotRecall := make(chan struct{})
	m.SubscribeRecall("reader", obj, func(id uint64) {
		recallID = id
		close(gotRecall)
	})

	done := make(chan RecallResult, 1)
	go func() {
		res, err := m.Recall(context.Background(), obj, "writer", 5*time.Second)
		if err != nil {
			t.Error(err)
		}
		done <- res
	}()

	<-gotRecall
	// The mutation must still be blocked: nothing has acked yet.
	select {
	case <-done:
		t.Fatal("Recall returned before the holder acknowledged — §10.6 requires it to block")
	case <-time.After(50 * time.Millisecond):
	}

	m.AckRecall(recallID, "reader")

	select {
	case res := <-done:
		if len(res.Acked) != 1 || res.Acked[0] != "reader" {
			t.Fatalf("expected reader in Acked, got %+v", res)
		}
		if len(res.TimedOut) != 0 {
			t.Fatalf("expected nothing timed out, got %+v", res.TimedOut)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Recall did not return after the holder acknowledged")
	}
}

// TestRecallProceedsOnDeadlineWhenHolderNeverAcks is the liveness half.
// §10.6 bounds the wait by DRECALL precisely so that one unreachable
// client cannot deadlock every writer behind it.
func TestRecallProceedsOnDeadlineWhenHolderNeverAcks(t *testing.T) {
	m, _ := newTestManager()
	const obj = "inode:1"

	m.Grant("silent", obj)
	m.SubscribeRecall("silent", obj, func(uint64) {}) // never acks

	start := time.Now()
	res, err := m.Recall(context.Background(), obj, "writer", 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("Recall returned after %s, before the DRECALL deadline", elapsed)
	}
	if len(res.TimedOut) != 1 || res.TimedOut[0] != "silent" {
		t.Fatalf("expected silent to be reported as timed out, got %+v", res)
	}
	if len(res.Acked) != 0 {
		t.Fatalf("expected no acks, got %+v", res.Acked)
	}
}

// TestRecallRevokesLeaseBeforeWaitingForAck is the safety property that
// makes a timeout survivable: the server-side grant is dropped when the
// recall is issued, not when the ack arrives. Otherwise a holder that
// never answers would keep a lease this Manager still considered valid
// for the whole DRECALL window, and could serve a read from it.
func TestRecallRevokesLeaseBeforeWaitingForAck(t *testing.T) {
	m, _ := newTestManager()
	const obj = "inode:1"

	m.Grant("reader", obj)
	if _, ok := m.TrustedVersion("reader", obj); !ok {
		t.Fatal("precondition: reader should hold a valid lease")
	}

	checked := make(chan bool, 1)
	m.SubscribeRecall("reader", obj, func(uint64) {
		_, ok := m.TrustedVersion("reader", obj)
		checked <- ok
	})

	go m.Recall(context.Background(), obj, "writer", 50*time.Millisecond)

	select {
	case stillTrusted := <-checked:
		if stillTrusted {
			t.Fatal("lease was still trusted while the recall was in flight — a holder could serve a stale read")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("recall handler never fired")
	}
}

// TestRecallSkipsTheMutatorAndExpiredHolders: the writer's own lease is
// not recalled (it is the one making the change), and a holder whose
// lease already lapsed has nothing to give up.
func TestRecallSkipsTheMutatorAndExpiredHolders(t *testing.T) {
	m, clk := newTestManager()
	const obj = "inode:1"

	m.Grant("stale", obj)
	m.SubscribeRecall("stale", obj, func(uint64) {
		t.Error("an expired holder should not be recalled")
	})
	clk.advance(10 * time.Second) // past the 5s lease

	m.Grant("writer", obj)
	m.SubscribeRecall("writer", obj, func(uint64) {
		t.Error("the mutator should not recall itself")
	})

	res, err := m.Recall(context.Background(), obj, "writer", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Acked) != 0 || len(res.TimedOut) != 0 {
		t.Fatalf("expected nobody to be recalled, got %+v", res)
	}
}

// TestRecallWaitsForEveryHolder: with several holders, the mutation is
// released only by the last ack, not the first.
func TestRecallWaitsForEveryHolder(t *testing.T) {
	m, _ := newTestManager()
	const obj = "inode:1"

	holders := []string{"a", "b", "c"}
	var mu sync.Mutex
	ids := map[string]uint64{}
	fired := make(chan string, len(holders))
	for _, h := range holders {
		h := h
		m.Grant(h, obj)
		m.SubscribeRecall(h, obj, func(id uint64) {
			mu.Lock()
			ids[h] = id
			mu.Unlock()
			fired <- h
		})
	}

	done := make(chan RecallResult, 1)
	go func() {
		res, _ := m.Recall(context.Background(), obj, "writer", 5*time.Second)
		done <- res
	}()

	for range holders {
		<-fired
	}
	// Ack all but the last; the mutation must stay blocked.
	mu.Lock()
	idA, idB, idC := ids["a"], ids["b"], ids["c"]
	mu.Unlock()
	m.AckRecall(idA, "a")
	m.AckRecall(idB, "b")
	select {
	case <-done:
		t.Fatal("Recall returned with one holder still un-acked")
	case <-time.After(50 * time.Millisecond):
	}

	m.AckRecall(idC, "c")
	select {
	case res := <-done:
		if len(res.Acked) != 3 {
			t.Fatalf("expected all three holders acked, got %+v", res)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Recall did not return after the final ack")
	}
}

// TestLateAckAfterTimeoutIsHarmless: an ack that arrives after the
// mutation already gave up must not panic or corrupt state.
func TestLateAckAfterTimeoutIsHarmless(t *testing.T) {
	m, _ := newTestManager()
	const obj = "inode:1"

	m.Grant("slow", obj)
	var captured uint64
	got := make(chan struct{})
	m.SubscribeRecall("slow", obj, func(id uint64) {
		captured = id
		close(got)
	})

	res, err := m.Recall(context.Background(), obj, "writer", 30*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.TimedOut) != 1 {
		t.Fatalf("expected a timeout, got %+v", res)
	}
	<-got
	m.AckRecall(captured, "slow") // must be a no-op, not a panic
}
