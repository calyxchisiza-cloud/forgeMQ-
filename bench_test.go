package forgemq

import (
	"context"
	"testing"
)

func BenchmarkPublish(b *testing.B) {
	br := New()
	if err := br.CreateTopic("orders.created"); err != nil {
		b.Fatal(err)
	}
	msg := Message{Topic: "orders.created", Payload: []byte("order")}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := br.Publish(msg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPublishDisk(b *testing.B) {
	br, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer br.Close()
	if err := br.CreateTopic("orders.created"); err != nil {
		b.Fatal(err)
	}
	msg := Message{Topic: "orders.created", Payload: []byte("order")}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := br.Publish(msg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPublishBatchDisk(b *testing.B) {
	br, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer br.Close()
	if err := br.CreateTopic("orders.created"); err != nil {
		b.Fatal(err)
	}
	msgs := make([]Message, 32)
	for i := range msgs {
		msgs[i] = Message{Topic: "orders.created", Payload: []byte("order")}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := br.PublishBatch(msgs); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConcurrentPublish(b *testing.B) {
	br := New()
	if err := br.CreateTopic("orders.created"); err != nil {
		b.Fatal(err)
	}
	msg := Message{Topic: "orders.created", Payload: []byte("order")}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := br.Publish(msg); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkConsume(b *testing.B) {
	br := NewWith(Config{Prefetch: 64})
	if err := br.CreateTopic("orders.created"); err != nil {
		b.Fatal(err)
	}
	msg := Message{Topic: "orders.created", Payload: []byte("order")}
	for i := 0; i < b.N; i++ {
		if _, err := br.Publish(msg); err != nil {
			b.Fatal(err)
		}
	}
	sub, err := br.Subscribe("orders.created", "accounting")
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	left := b.N
	for left > 0 {
		n := 64
		if n > left {
			n = left
		}
		got, err := sub.ReceiveN(ctx, n)
		if err != nil {
			b.Fatal(err)
		}
		for _, d := range got {
			if err := d.Ack(); err != nil {
				b.Fatal(err)
			}
		}
		left -= len(got)
	}
}
