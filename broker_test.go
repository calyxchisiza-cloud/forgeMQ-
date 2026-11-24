package forgemq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestTwoGroupsEachReceiveTheLog(t *testing.T) {
	b := New()
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
	payload := []byte(`{"order_id":1001}`)
	published, err := b.Publish(Message{Topic: "orders.created", Key: []byte("order:1001"), Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	if published.Offset != 0 || published.ID == 0 || string(published.Payload) != `{"order_id":1001}` {
		t.Fatalf("published %+v", published)
	}
	second, err := b.Publish(Message{Topic: "orders.created", Payload: []byte("next")})
	if err != nil {
		t.Fatal(err)
	}
	if second.Offset != 1 || second.ID == published.ID {
		t.Fatalf("second %+v", second)
	}

	ctx := context.Background()
	for _, c := range []*Consumer{accounting, analytics} {
		first, err := c.Receive(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if c.Offset() != 0 {
			t.Fatalf("%s offset moved before ack", c.Group())
		}
		if first.Offset != 0 || string(first.Payload) != `{"order_id":1001}` || string(first.Key) != "order:1001" {
			t.Fatalf("%s first %+v", c.Group(), first)
		}
		if err := first.Ack(); err != nil {
			t.Fatal(err)
		}
		if c.Offset() != 1 {
			t.Fatalf("%s offset %d", c.Group(), c.Offset())
		}
		next, err := c.Receive(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if next.Offset != 1 || string(next.Payload) != "next" {
			t.Fatalf("%s next %+v", c.Group(), next)
		}
		if err := next.Ack(); err != nil {
			t.Fatal(err)
		}
		if err := next.Ack(); !errors.Is(err, ErrSettled) {
			t.Fatalf("%s second ack %v", c.Group(), err)
		}
		if c.Offset() != 2 {
			t.Fatalf("%s offset %d", c.Group(), c.Offset())
		}
	}
}

func TestGroupMembersShareOneOffset(t *testing.T) {
	b := New()
	if err := b.CreateTopic("orders.created"); err != nil {
		t.Fatal(err)
	}
	a, err := b.Subscribe("orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	other, err := b.Subscribe("orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, err := b.Publish(Message{Topic: "orders.created", Payload: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	var mu sync.Mutex
	got := map[uint64]int{}
	var wg sync.WaitGroup
	take := func(c *Consumer) {
		defer wg.Done()
		for n := 0; n < 2; n++ {
			msg, err := c.Receive(ctx)
			if err != nil {
				t.Error(err)
				return
			}
			if err := msg.Ack(); err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			got[msg.Offset]++
			mu.Unlock()
		}
	}
	wg.Add(2)
	go take(a)
	go take(other)
	wg.Wait()
	if len(got) != 4 {
		t.Fatalf("offsets %v", got)
	}
	for offset, count := range got {
		if count != 1 || offset > 3 {
			t.Fatalf("offsets %v", got)
		}
	}
}

func TestLateSubscriberReadsFromTheStart(t *testing.T) {
	b := New()
	if err := b.CreateTopic("payments.completed"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(Message{Topic: "payments.completed", Payload: []byte("1001")}); err != nil {
		t.Fatal(err)
	}
	email, err := b.Subscribe("payments.completed", "email")
	if err != nil {
		t.Fatal(err)
	}
	msg, err := email.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if msg.Offset != 0 || string(msg.Payload) != "1001" {
		t.Fatalf("%+v", msg)
	}
}

func TestReceiveWaitsUntilPublish(t *testing.T) {
	b := New()
	if err := b.CreateTopic("users.registered"); err != nil {
		t.Fatal(err)
	}
	c, err := b.Subscribe("users.registered", "welcome")
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan Message, 1)
	go func() {
		msg, err := c.Receive(context.Background())
		if err != nil {
			t.Error(err)
			return
		}
		got <- msg.Message
	}()
	time.Sleep(20 * time.Millisecond)
	select {
	case msg := <-got:
		t.Fatalf("delivered early %+v", msg)
	default:
	}
	if _, err := b.Publish(Message{Topic: "users.registered", Payload: []byte("ada")}); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-got:
		if string(msg.Payload) != "ada" {
			t.Fatalf("%+v", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("receive did not return")
	}
}

func TestCancelAndCloseUnblockReceive(t *testing.T) {
	b := New()
	if err := b.CreateTopic("orders.created"); err != nil {
		t.Fatal(err)
	}
	c, err := b.Subscribe("orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		_, err := c.Receive(ctx)
		errc <- err
	}()
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
		_, err := c.Receive(context.Background())
		errc <- err
	}()
	b.Close()
	select {
	case err := <-errc:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("close %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("close did not unblock")
	}
	if _, err := b.Publish(Message{Topic: "orders.created"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("publish after close %v", err)
	}
}

func TestRejectedNamesAndMissingTopic(t *testing.T) {
	b := New()
	if err := b.CreateTopic(""); !errors.Is(err, ErrInvalidName) {
		t.Fatalf("empty %v", err)
	}
	if err := b.CreateTopic("orders.created"); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic("orders.created"); !errors.Is(err, ErrTopicExists) {
		t.Fatalf("exists %v", err)
	}
	if _, err := b.Publish(Message{Topic: "missing"}); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("missing %v", err)
	}
	if _, err := b.Subscribe("orders.created", "bad name"); !errors.Is(err, ErrInvalidName) {
		t.Fatalf("group %v", err)
	}
}

func TestHeadersAreCopied(t *testing.T) {
	b := New()
	if err := b.CreateTopic("notifications.email"); err != nil {
		t.Fatal(err)
	}
	headers := map[string]string{"kind": "receipt"}
	published, err := b.Publish(Message{Topic: "notifications.email", Headers: headers, Payload: []byte("body")})
	if err != nil {
		t.Fatal(err)
	}
	headers["kind"] = "changed"
	c, err := b.Subscribe("notifications.email", "email")
	if err != nil {
		t.Fatal(err)
	}
	msg, err := c.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if published.Headers["kind"] != "receipt" || msg.Headers["kind"] != "receipt" {
		t.Fatalf("published %v delivered %v", published.Headers, msg.Headers)
	}
}

func TestNackRetriesThenDeadLetters(t *testing.T) {
	b := NewWith(Config{MaxAttempts: 2, AckTimeout: time.Hour})
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
	if _, err := b.Publish(Message{Topic: "orders.created", Key: []byte("k"), Payload: []byte("body")}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(Message{Topic: "orders.created", Payload: []byte("next")}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first, err := accounting.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first.Headers["forgemq-attempt"] != "" {
		t.Fatalf("first attempt header %q", first.Headers["forgemq-attempt"])
	}
	if err := first.Nack(true); err != nil {
		t.Fatal(err)
	}
	if accounting.Offset() != 0 {
		t.Fatalf("offset %d", accounting.Offset())
	}
	if err := first.Nack(true); !errors.Is(err, ErrSettled) {
		t.Fatalf("old nack %v", err)
	}
	again, err := accounting.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if again.Offset != 0 || again.Headers["forgemq-attempt"] != "2" || string(again.Payload) != "body" {
		t.Fatalf("%+v", again.Message)
	}
	other, err := analytics.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if other.Offset != 0 || string(other.Payload) != "body" {
		t.Fatalf("analytics %+v", other.Message)
	}
	if err := other.Ack(); err != nil {
		t.Fatal(err)
	}
	if err := again.Nack(true); err != nil {
		t.Fatal(err)
	}
	if accounting.Offset() != 1 {
		t.Fatalf("offset after dead letter %d", accounting.Offset())
	}
	following, err := accounting.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if following.Offset != 1 || string(following.Payload) != "next" {
		t.Fatalf("following %+v", following.Message)
	}
	reader, err := b.Subscribe("orders.created.dlq", "reader")
	if err != nil {
		t.Fatal(err)
	}
	dead, err := reader.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(dead.Payload) != "body" || string(dead.Key) != "k" || dead.Offset != 0 {
		t.Fatalf("dead %+v", dead.Message)
	}
	if dead.Headers["forgemq-original-topic"] != "orders.created" ||
		dead.Headers["forgemq-original-offset"] != "0" ||
		dead.Headers["forgemq-attempts"] != "2" ||
		dead.Headers["forgemq-reason"] != "attempts" {
		t.Fatalf("headers %v", dead.Headers)
	}
}

func TestNackFalseDeadLettersImmediately(t *testing.T) {
	b := NewWith(Config{MaxAttempts: 5, AckTimeout: time.Hour})
	if err := b.CreateTopic("orders.created"); err != nil {
		t.Fatal(err)
	}
	c, err := b.Subscribe("orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(Message{Topic: "orders.created", Payload: []byte("skip")}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	d, err := c.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Nack(false); err != nil {
		t.Fatal(err)
	}
	if c.Offset() != 1 {
		t.Fatalf("offset %d", c.Offset())
	}
	reader, err := b.Subscribe("orders.created.dlq", "reader")
	if err != nil {
		t.Fatal(err)
	}
	dead, err := reader.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(dead.Payload) != "skip" || dead.Headers["forgemq-reason"] != "rejected" || dead.Headers["forgemq-attempts"] != "1" {
		t.Fatalf("%+v", dead.Message)
	}
}

func TestTimeoutHandsTheOffsetOutAgain(t *testing.T) {
	b := NewWith(Config{AckTimeout: 30 * time.Millisecond, MaxAttempts: 3})
	if err := b.CreateTopic("orders.created"); err != nil {
		t.Fatal(err)
	}
	c, err := b.Subscribe("orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(Message{Topic: "orders.created", Payload: []byte("late")}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	first, err := c.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	again, err := c.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if again.Offset != 0 || again.Headers["forgemq-attempt"] != "2" || string(again.Payload) != "late" {
		t.Fatalf("%+v", again.Message)
	}
	if err := first.Ack(); !errors.Is(err, ErrSettled) {
		t.Fatalf("ack after timeout %v", err)
	}
	if err := again.Ack(); err != nil {
		t.Fatal(err)
	}
	if c.Offset() != 1 {
		t.Fatalf("offset %d", c.Offset())
	}
}

func TestTimeoutDeadLettersAtTheAttemptLimit(t *testing.T) {
	b := NewWith(Config{AckTimeout: 30 * time.Millisecond, MaxAttempts: 1})
	if err := b.CreateTopic("orders.created"); err != nil {
		t.Fatal(err)
	}
	c, err := b.Subscribe("orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(Message{Topic: "orders.created", Payload: []byte("gone")}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	first, err := c.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var reader *Consumer
	for {
		reader, err = b.Subscribe("orders.created.dlq", "reader")
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	dead, err := reader.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(dead.Payload) != "gone" || dead.Headers["forgemq-reason"] != "timeout" || dead.Headers["forgemq-attempts"] != "1" {
		t.Fatalf("%+v headers %v", dead.Message, dead.Headers)
	}
	if err := first.Ack(); !errors.Is(err, ErrSettled) {
		t.Fatalf("ack after timeout %v", err)
	}
	if c.Offset() != 1 {
		t.Fatalf("offset %d", c.Offset())
	}
}

func TestPrefetchHoldsTheNextReceive(t *testing.T) {
	b := NewWith(Config{Prefetch: 1, AckTimeout: time.Hour})
	if err := b.CreateTopic("orders.created"); err != nil {
		t.Fatal(err)
	}
	c, err := b.Subscribe("orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{"one", "two"} {
		if _, err := b.Publish(Message{Topic: "orders.created", Payload: []byte(body)}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := c.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	_, err = c.Receive(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second receive %v", err)
	}
	if err := first.Ack(); err != nil {
		t.Fatal(err)
	}
	next, err := c.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if next.Offset != 1 || string(next.Payload) != "two" {
		t.Fatalf("%+v", next.Message)
	}
}

func TestOutOfOrderAckAdvancesThroughTheGap(t *testing.T) {
	b := NewWith(Config{Prefetch: 2, AckTimeout: time.Hour})
	if err := b.CreateTopic("orders.created"); err != nil {
		t.Fatal(err)
	}
	c, err := b.Subscribe("orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{"one", "two"} {
		if _, err := b.Publish(Message{Topic: "orders.created", Payload: []byte(body)}); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	first, err := c.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first.Offset != 0 || second.Offset != 1 {
		t.Fatalf("offsets %d %d", first.Offset, second.Offset)
	}
	if err := second.Ack(); err != nil {
		t.Fatal(err)
	}
	if c.Offset() != 0 {
		t.Fatalf("offset moved across a gap %d", c.Offset())
	}
	if err := first.Ack(); err != nil {
		t.Fatal(err)
	}
	if c.Offset() != 2 {
		t.Fatalf("offset %d", c.Offset())
	}
}
