# ForgeMQ

ForgeMQ accepts a message once and keeps an ordered log for each topic. A subscription group reads that log at its own offset. Consumers in one group share that group's in-flight messages, so two groups each see every message and two consumers in the same group do not.

This build keeps the log in memory. `Receive` returns a delivery and leaves the group offset where it is. `Ack` moves the offset. `Nack` sends the same offset again, or appends it to the topic's dead-letter log when requeue is false or the attempt limit is reached. A delivery that is never acknowledged is handed out again when its timeout expires. `Publish` returns only after the broker has accepted the message. Disk and the network come later.

```go
broker := forgemq.New()
err := broker.CreateTopic("orders.created")
accounting, err := broker.Subscribe("orders.created", "accounting")
analytics, err := broker.Subscribe("orders.created", "analytics")

msg, err := broker.Publish(forgemq.Message{
    Topic:   "orders.created",
    Key:     []byte("order:1001"),
    Payload: []byte(`{"order_id":1001}`),
})

delivery, err := accounting.Receive(ctx)
err = delivery.Ack()
```

`Receive` waits while the group already has its prefetch count in flight. The default is one, so settle the current delivery before calling `Receive` again on the same goroutine. `NewWith` changes the acknowledgement timeout, the attempt limit, and prefetch. Zero values keep the defaults of 30 seconds, 5 attempts, and prefetch 1.

A topic name and a group name start with a letter or `_` and may then contain letters, digits, `.`, `_`, and `-`.

## Run

```
go run ./cmd/demo
```

The demo prints the same `orders.created` message for the accounting group and the analytics group.

## Test

```
go test ./...
go test -race ./...
```

See [docs/architecture.md](docs/architecture.md).
