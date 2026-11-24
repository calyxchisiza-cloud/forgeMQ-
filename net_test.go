package forgemq

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func testPair(t *testing.T, b *Broker) (*Server, *Client) {
	t.Helper()
	if b == nil {
		b = New()
	}
	s, err := Listen("127.0.0.1:0", b)
	if err != nil {
		t.Fatal(err)
	}
	c, err := Connect(s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	t.Cleanup(func() { s.Close() })
	t.Cleanup(func() { c.Close() })
	return s, c
}

func TestClientsInTwoGroupsEachReceiveTheLog(t *testing.T) {
	_, c := testPair(t, nil)
	ctx := context.Background()
	if err := c.CreateTopic(ctx, "orders.created"); err != nil {
		t.Fatal(err)
	}
	if err := c.CreateTopic(ctx, "orders.created"); !errors.Is(err, ErrTopicExists) {
		t.Fatalf("exists %v", err)
	}
	published, err := c.Publish(ctx, Message{
		Topic:   "orders.created",
		Key:     nil,
		Payload: []byte{},
		Headers: map[string]string{"kind": "created"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if published.ID == 0 || published.Offset != 0 || published.Timestamp.IsZero() {
		t.Fatalf("%+v", published)
	}
	if published.Key != nil || published.Payload == nil || len(published.Payload) != 0 || published.Headers["kind"] != "created" {
		t.Fatalf("%+v", published)
	}
	analytics, err := Connect(c.addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { analytics.Close() })
	accountingSub, err := c.Subscribe(ctx, "orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	analyticsSub, err := analytics.Subscribe(ctx, "orders.created", "analytics")
	if err != nil {
		t.Fatal(err)
	}
	for _, sub := range []*Subscription{accountingSub, analyticsSub} {
		ev, err := sub.Receive(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if ev.Offset != 0 || ev.Key != nil || string(ev.Headers["kind"]) != "created" {
			t.Fatalf("%+v", ev.Message)
		}
		if err := ev.Ack(); err != nil {
			t.Fatal(err)
		}
		if err := ev.Ack(); !errors.Is(err, ErrSettled) {
			t.Fatalf("second ack %v", err)
		}
	}
}

func TestGroupMembersShareWorkOverTheNetwork(t *testing.T) {
	s, c := testPair(t, nil)
	ctx := context.Background()
	if err := c.CreateTopic(ctx, "orders.created"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		msg, err := c.Publish(ctx, Message{Topic: "orders.created", Payload: []byte{byte(i)}})
		if err != nil {
			t.Fatal(err)
		}
		if msg.Offset != uint64(i) {
			t.Fatalf("offset %d", msg.Offset)
		}
	}
	other, err := Connect(s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { other.Close() })
	a, err := c.Subscribe(ctx, "orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	b, err := other.Subscribe(ctx, "orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	got := map[uint64]int{}
	var wg sync.WaitGroup
	take := func(sub *Subscription) {
		defer wg.Done()
		for n := 0; n < 2; n++ {
			ev, err := sub.Receive(ctx)
			if err != nil {
				t.Error(err)
				return
			}
			if err := ev.Ack(); err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			got[ev.Offset]++
			mu.Unlock()
		}
	}
	wg.Add(2)
	go take(a)
	go take(b)
	wg.Wait()
	if len(got) != 4 {
		t.Fatalf("offsets %v", got)
	}
}

func TestNackAndDeadLetterOverTheNetwork(t *testing.T) {
	_, c := testPair(t, NewWith(Config{MaxAttempts: 2, AckTimeout: time.Hour}))
	ctx := context.Background()
	if err := c.CreateTopic(ctx, "orders.created"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Publish(ctx, Message{Topic: "orders.created", Payload: []byte("body")}); err != nil {
		t.Fatal(err)
	}
	sub, err := c.Subscribe(ctx, "orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	ev, err := sub.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := ev.Nack(true); err != nil {
		t.Fatal(err)
	}
	again, err := sub.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if again.Offset != 0 || again.Headers["forgemq-attempt"] != "2" {
		t.Fatalf("%+v", again.Message)
	}
	if err := again.Nack(false); err != nil {
		t.Fatal(err)
	}
	reader, err := c.Subscribe(ctx, "orders.created.dlq", "reader")
	if err != nil {
		t.Fatal(err)
	}
	dead, err := reader.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(dead.Payload) != "body" || dead.Headers["forgemq-reason"] != "rejected" {
		t.Fatalf("%+v", dead.Message)
	}
}

func TestDroppedConnectionIsDeliveredAgain(t *testing.T) {
	s, c := testPair(t, nil)
	ctx := context.Background()
	if err := c.CreateTopic(ctx, "orders.created"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Publish(ctx, Message{Topic: "orders.created", Payload: []byte("again")}); err != nil {
		t.Fatal(err)
	}
	sub, err := c.Subscribe(ctx, "orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	ev, err := sub.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	conn.Close()
	if err := ev.Ack(); !errors.Is(err, ErrSettled) {
		t.Fatalf("ack after drop %v", err)
	}
	wait, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	again, err := sub.Receive(wait)
	if err != nil {
		t.Fatal(err)
	}
	if again.Offset != 0 || string(again.Payload) != "again" || again.Headers["forgemq-attempt"] != "2" {
		t.Fatalf("%+v", again.Message)
	}
	if err := again.Ack(); err != nil {
		t.Fatal(err)
	}
	quiet, cancelQuiet := context.WithTimeout(context.Background(), 50*time.Millisecond)
	_, err = sub.Receive(quiet)
	cancelQuiet()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("after ack %v", err)
	}
	if s.Addr() == "" {
		t.Fatal("missing address")
	}
}

func TestAckSurvivesARestart(t *testing.T) {
	dir := t.TempDir()
	b, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Listen("127.0.0.1:0", b)
	if err != nil {
		t.Fatal(err)
	}
	c, err := Connect(s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := c.CreateTopic(ctx, "orders.created"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Publish(ctx, Message{Topic: "orders.created", Payload: []byte("kept")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Publish(ctx, Message{Topic: "orders.created", Payload: []byte("open")}); err != nil {
		t.Fatal(err)
	}
	sub, err := c.Subscribe(ctx, "orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	ev, err := sub.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := ev.Ack(); err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatal(err)
	}
	c.Close()
	s.Close()
	b.Close()

	b, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	s, err = Listen("127.0.0.1:0", b)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c, err = Connect(s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	sub, err = c.Subscribe(ctx, "orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	ev, err = sub.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ev.Offset != 1 || string(ev.Payload) != "open" {
		t.Fatalf("%+v", ev.Message)
	}
}

func TestCancelAndServerCloseUnblockReceive(t *testing.T) {
	s, c := testPair(t, nil)
	ctx := context.Background()
	if err := c.CreateTopic(ctx, "orders.created"); err != nil {
		t.Fatal(err)
	}
	sub, err := c.Subscribe(ctx, "orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		_, err := sub.Receive(cancelled)
		errc <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancel did not unblock")
	}
	errc = make(chan error, 1)
	go func() {
		_, err := sub.Receive(context.Background())
		errc <- err
	}()
	time.Sleep(20 * time.Millisecond)
	s.Close()
	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("server close returned a delivery")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server close did not unblock")
	}
}

func TestRejectedCommands(t *testing.T) {
	_, c := testPair(t, nil)
	ctx := context.Background()
	if err := c.CreateTopic(ctx, "bad name"); !errors.Is(err, ErrInvalidName) {
		t.Fatalf("name %v", err)
	}
	if _, err := c.Publish(ctx, Message{Topic: "missing", Payload: []byte("x")}); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("missing %v", err)
	}
	if _, err := Listen("127.0.0.1:0", nil); err == nil {
		t.Fatal("nil broker was accepted")
	}
}

func TestConcurrentPublish(t *testing.T) {
	_, c := testPair(t, nil)
	ctx := context.Background()
	if err := c.CreateTopic(ctx, "orders.created"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	got := make(chan uint64, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg, err := c.Publish(ctx, Message{Topic: "orders.created", Payload: []byte("x")})
			if err != nil {
				t.Error(err)
				return
			}
			got <- msg.Offset
		}()
	}
	wg.Wait()
	close(got)
	seen := map[uint64]bool{}
	for offset := range got {
		if seen[offset] || offset > 7 {
			t.Fatalf("offset %d", offset)
		}
		seen[offset] = true
	}
	if len(seen) != 8 {
		t.Fatalf("offsets %v", seen)
	}
}

func TestFrameRejectsABadChecksum(t *testing.T) {
	buf, err := frameBytes(7, opAck, []byte{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	corr, op, payload, err := readFrame(bytes.NewReader(buf))
	if err != nil || corr != 7 || op != opAck || !bytes.Equal(payload, []byte{1, 2, 3}) {
		t.Fatalf("corr %d op %d payload %v err %v", corr, op, payload, err)
	}
	buf[4] ^= 0xff
	if _, _, _, err := readFrame(bytes.NewReader(buf)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("checksum %v", err)
	}
}
