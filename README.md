# ForgeMQ

ForgeMQ accepts a message once and keeps an ordered log for each topic. A subscription group reads that log at its own offset. Consumers in one group take the next message one at a time, so two groups each see every message and two consumers in the same group do not.

This first build keeps the log in memory. `Receive` advances the group offset when it returns a message. A consumer that stops after `Receive` does not get that message again. Acknowledgements, disk, and the network come later.

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
```

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
