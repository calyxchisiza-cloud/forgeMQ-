package forgemq

import (
	"context"
	"errors"
	"testing"
)

func TestInspectReadsTheLogWithoutMovingAGroup(t *testing.T) {
	b := New()
	if err := b.CreateTopic("orders.created"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(Message{Topic: "orders.created", Key: []byte("order:1"), Payload: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(Message{Topic: "orders.created", Payload: []byte("two")}); err != nil {
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
	if err := first.Ack(); err != nil {
		t.Fatal(err)
	}
	page, err := b.Inspect("orders.created", 0, 10, false)
	if err != nil {
		t.Fatal(err)
	}
	if page.Base != 0 || page.End != 2 || page.From != 0 || len(page.Messages) != 2 {
		t.Fatalf("page %+v", page)
	}
	if string(page.Messages[0].Payload) != "one" || string(page.Messages[0].Key) != "order:1" {
		t.Fatalf("first %+v", page.Messages[0])
	}
	if accounting.Offset() != 1 || analytics.Offset() != 0 {
		t.Fatalf("offsets %d %d", accounting.Offset(), analytics.Offset())
	}
	var accountingInfo, analyticsInfo GroupInfo
	for _, g := range page.Groups {
		switch g.Name {
		case "accounting":
			accountingInfo = g
		case "analytics":
			analyticsInfo = g
		}
	}
	if accountingInfo.Commit != 1 || accountingInfo.Behind != 1 {
		t.Fatalf("accounting %+v", accountingInfo)
	}
	if analyticsInfo.Commit != 0 || analyticsInfo.Behind != 2 {
		t.Fatalf("analytics %+v", analyticsInfo)
	}
	tail, err := b.Inspect("orders.created", 0, 1, true)
	if err != nil {
		t.Fatal(err)
	}
	if tail.From != 1 || len(tail.Messages) != 1 || string(tail.Messages[0].Payload) != "two" {
		t.Fatalf("tail %+v", tail)
	}
	if _, err := b.Inspect("missing", 0, 10, false); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("missing %v", err)
	}
	topics, err := b.Topics()
	if err != nil {
		t.Fatal(err)
	}
	if len(topics) != 1 || topics[0].Records != 2 {
		t.Fatalf("topics %+v", topics)
	}
}
