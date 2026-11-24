package main

import (
	"context"
	"fmt"
	"os"

	"forgemq"
)

func main() {
	broker := forgemq.New()
	defer broker.Close()
	if err := broker.CreateTopic("orders.created"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	accounting, err := broker.Subscribe("orders.created", "accounting")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	analytics, err := broker.Subscribe("orders.created", "analytics")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if _, err := broker.Publish(forgemq.Message{
		Topic:   "orders.created",
		Key:     []byte("order:1001"),
		Payload: []byte(`{"order_id":1001}`),
	}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx := context.Background()
	for _, consumer := range []*forgemq.Consumer{accounting, analytics} {
		msg, err := consumer.Receive(ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Printf("%s %s %d %s\n", consumer.Group(), msg.Topic, msg.Offset, msg.Payload)
	}
}
