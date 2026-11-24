package forgemq

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPublishBatchKeepsOrderAcrossARestart(t *testing.T) {
	dir := t.TempDir()
	b, err := OpenWith(Config{Dir: dir, SegmentBytes: 200})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic("orders.created"); err != nil {
		t.Fatal(err)
	}
	msgs := make([]Message, 8)
	for i := range msgs {
		msgs[i] = Message{Topic: "orders.created", Payload: []byte("item")}
	}
	got, err := b.PublishBatch(msgs)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 8 || got[0].Offset != 0 || got[7].Offset != 7 {
		t.Fatalf("batch %+v", got)
	}
	if st := b.Stats(); st.Published != 8 {
		t.Fatalf("published %d", st.Published)
	}
	b.Close()

	b, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	sub, err := b.Subscribe("orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := 0; i < 8; i++ {
		ev, err := sub.Receive(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if ev.Offset != uint64(i) || string(ev.Payload) != "item" {
			t.Fatalf("reloaded %+v", ev.Message)
		}
		if err := ev.Ack(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPublishBatchRejectsAMixedTopic(t *testing.T) {
	b := New()
	if err := b.CreateTopic("orders.created"); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic("payments.completed"); err != nil {
		t.Fatal(err)
	}
	_, err := b.PublishBatch([]Message{
		{Topic: "orders.created", Payload: []byte("a")},
		{Topic: "payments.completed", Payload: []byte("b")},
	})
	if !errors.Is(err, ErrBadRequest) {
		t.Fatalf("mixed %v", err)
	}
	if _, err := b.PublishBatch(nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	sub, err := b.Subscribe("orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Receive(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stored %v", err)
	}
}

func TestReceiveNStopsAtPrefetch(t *testing.T) {
	b := NewWith(Config{Prefetch: 2})
	if err := b.CreateTopic("orders.created"); err != nil {
		t.Fatal(err)
	}
	msgs := make([]Message, 5)
	for i := range msgs {
		msgs[i] = Message{Topic: "orders.created", Payload: []byte{byte('a' + i)}}
	}
	if _, err := b.PublishBatch(msgs); err != nil {
		t.Fatal(err)
	}
	sub, err := b.Subscribe("orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first, err := sub.ReceiveN(ctx, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || first[0].Offset != 0 || first[1].Offset != 1 {
		t.Fatalf("first %d", len(first))
	}
	for _, d := range first {
		if err := d.Ack(); err != nil {
			t.Fatal(err)
		}
	}
	rest, err := sub.ReceiveN(ctx, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 2 || rest[0].Offset != 2 {
		t.Fatalf("rest %+v", rest)
	}
	if _, err := sub.ReceiveN(ctx, 0); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("zero %v", err)
	}
}

func TestClientPublishBatchIsSharedByTheGroup(t *testing.T) {
	_, c := testPair(t, nil)
	ctx := context.Background()
	if err := c.CreateTopic(ctx, "orders.created"); err != nil {
		t.Fatal(err)
	}
	got, err := c.PublishBatch(ctx, []Message{
		{Topic: "orders.created", Payload: []byte("one")},
		{Topic: "orders.created", Payload: []byte("two")},
		{Topic: "orders.created", Payload: []byte("three")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[2].Offset != 2 {
		t.Fatalf("offsets %+v", got)
	}
	sub, err := c.Subscribe(ctx, "orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"one", "two", "three"} {
		ev, err := sub.Receive(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if ev.Offset != uint64(i) || string(ev.Payload) != want {
			t.Fatalf("delivery %+v", ev.Message)
		}
		if err := ev.Ack(); err != nil {
			t.Fatal(err)
		}
	}
}
