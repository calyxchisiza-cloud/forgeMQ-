package forgemq

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRestartRedeliversUntilAck(t *testing.T) {
	dir := t.TempDir()
	b, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic("orders.created"); err != nil {
		t.Fatal(err)
	}
	published, err := b.Publish(Message{
		Topic:   "orders.created",
		Key:     []byte("order:1001"),
		Payload: []byte(`{"order_id":1001}`),
		Headers: map[string]string{"kind": "created"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(Message{Topic: "orders.created", Key: nil, Payload: []byte{}}); err != nil {
		t.Fatal(err)
	}
	b.Close()

	b, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	accounting, err := b.Subscribe("orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	analytics, err := b.Subscribe("orders.created", "analytics")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first, err := accounting.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first.Offset != 0 || first.ID != published.ID || string(first.Key) != "order:1001" || first.Headers["kind"] != "created" {
		t.Fatalf("%+v", first.Message)
	}
	if !first.Timestamp.Equal(published.Timestamp) {
		t.Fatalf("timestamp %s want %s", first.Timestamp, published.Timestamp)
	}
	if err := first.Ack(); err != nil {
		t.Fatal(err)
	}
	second, err := accounting.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if second.Key != nil || second.Payload == nil || len(second.Payload) != 0 || second.Offset != 1 {
		t.Fatalf("%+v keynil %v", second.Message, second.Key == nil)
	}
	b.Close()

	b, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	accounting, err = b.Subscribe("orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	if accounting.Offset() != 1 {
		t.Fatalf("acked offset restarted at %d", accounting.Offset())
	}
	again, err := accounting.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if again.Offset != 1 || again.Key != nil || again.Payload == nil || len(again.Payload) != 0 {
		t.Fatalf("redelivered %+v keynil %v", again.Message, again.Key == nil)
	}
	analytics, err = b.Subscribe("orders.created", "analytics")
	if err != nil {
		t.Fatal(err)
	}
	if analytics.Offset() != 0 {
		t.Fatalf("new group offset %d", analytics.Offset())
	}
	skipped, err := analytics.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if skipped.Offset != 0 || string(skipped.Payload) != `{"order_id":1001}` {
		t.Fatalf("analytics %+v", skipped.Message)
	}
}

func TestEmptyTopicAndDeadLetterSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	b, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic("orders.created"); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic("users.registered"); err != nil {
		t.Fatal(err)
	}
	c, err := b.Subscribe("orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(Message{Topic: "orders.created", Payload: []byte("skip")}); err != nil {
		t.Fatal(err)
	}
	d, err := c.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Nack(false); err != nil {
		t.Fatal(err)
	}
	b.Close()

	b, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.CreateTopic("users.registered"); !errors.Is(err, ErrTopicExists) {
		t.Fatalf("empty topic %v", err)
	}
	c, err = b.Subscribe("orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	if c.Offset() != 1 {
		t.Fatalf("offset %d", c.Offset())
	}
	reader, err := b.Subscribe("orders.created.dlq", "reader")
	if err != nil {
		t.Fatal(err)
	}
	dead, err := reader.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(dead.Payload) != "skip" || dead.Headers["forgemq-reason"] != "rejected" || dead.Headers["forgemq-original-offset"] != "0" {
		t.Fatalf("%+v", dead.Message)
	}
}

func TestTornTailIsDiscarded(t *testing.T) {
	dir := t.TempDir()
	b, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic("orders.created"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(Message{Topic: "orders.created", Payload: []byte("kept")}); err != nil {
		t.Fatal(err)
	}
	b.Close()
	path := filepath.Join(dir, "data", "orders.created", "000000.log")
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xff, 0x01, 0x02}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	b, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("size %d want %d", after.Size(), before.Size())
	}
	c, err := b.Subscribe("orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	d, err := c.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(d.Payload) != "kept" {
		t.Fatalf("%+v", d.Message)
	}
	if _, err := b.Publish(Message{Topic: "orders.created", Payload: []byte("next")}); err != nil {
		t.Fatal(err)
	}
	if err := d.Ack(); err != nil {
		t.Fatal(err)
	}
	next, err := c.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if next.Offset != 1 || string(next.Payload) != "next" {
		t.Fatalf("%+v", next.Message)
	}
}

func TestBadChecksumStopsRecovery(t *testing.T) {
	dir := t.TempDir()
	b, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic("orders.created"); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{"one", "two"} {
		if _, err := b.Publish(Message{Topic: "orders.created", Payload: []byte(body)}); err != nil {
			t.Fatal(err)
		}
	}
	b.Close()
	path := filepath.Join(dir, "data", "orders.created", "000000.log")
	buf, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(buf) < 41 {
		t.Fatalf("segment %d bytes", len(buf))
	}
	buf[40] ^= 0xff
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("open %v", err)
	}
}

func TestSegmentsRollAndStayUntilEveryGroupPasses(t *testing.T) {
	dir := t.TempDir()
	b, err := OpenWith(Config{Dir: dir, SegmentBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic("orders.created"); err != nil {
		t.Fatal(err)
	}
	accounting, err := b.Subscribe("orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	analytics, err := b.Subscribe("orders.created", "analytics")
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{"one", "two"} {
		if _, err := b.Publish(Message{Topic: "orders.created", Payload: []byte(body)}); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	for _, body := range []string{"one", "two"} {
		d, err := accounting.Receive(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if string(d.Payload) != body {
			t.Fatalf("%+v", d.Message)
		}
		if err := d.Ack(); err != nil {
			t.Fatal(err)
		}
	}
	first := filepath.Join(dir, "data", "orders.created", "000000.log")
	second := filepath.Join(dir, "data", "orders.created", "000001.log")
	if _, err := os.Stat(first); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(second); err != nil {
		t.Fatal(err)
	}
	d, err := analytics.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if d.Offset != 0 || string(d.Payload) != "one" {
		t.Fatalf("%+v", d.Message)
	}
	if err := d.Ack(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(first); !os.IsNotExist(err) {
		t.Fatalf("old segment %v", err)
	}
	if _, err := os.Stat(second); err != nil {
		t.Fatal(err)
	}
	pending, err := analytics.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pending.Offset != 1 || string(pending.Payload) != "two" {
		t.Fatalf("%+v", pending.Message)
	}
	b.Close()

	b, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	accounting, err = b.Subscribe("orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	if accounting.Offset() != 2 {
		t.Fatalf("accounting %d", accounting.Offset())
	}
	wait, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	_, err = accounting.Receive(wait)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("accounting receive %v", err)
	}
	analytics, err = b.Subscribe("orders.created", "analytics")
	if err != nil {
		t.Fatal(err)
	}
	again, err := analytics.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if again.Offset != 1 || string(again.Payload) != "two" {
		t.Fatalf("redelivered %+v", again.Message)
	}
	fresh, err := b.Subscribe("orders.created", "audit")
	if err != nil {
		t.Fatal(err)
	}
	seen, err := fresh.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if seen.Offset != 1 {
		t.Fatalf("new group saw %+v", seen.Message)
	}
}

func TestOpenRequiresADirectory(t *testing.T) {
	if _, err := Open(""); err == nil {
		t.Fatal("empty directory was accepted")
	}
}
