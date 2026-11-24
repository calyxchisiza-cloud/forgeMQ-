# ForgeMQ

ForgeMQ accepts a message once and keeps an ordered log for each topic. A subscription group reads that log at its own offset. Consumers in one group share that group's in-flight messages, so two groups each see every message and two consumers in the same group do not.

`New` keeps the log in memory. `Open` stores each topic as append-only segment files and syncs a record before `Publish` returns. A restart reloads those files, ignores a torn record at the end of a segment, and delivers anything that was not acknowledged. `Receive` returns a delivery and leaves the group offset where it is. `Ack` moves the offset. `Nack` sends the same offset again, or appends it to the topic's dead-letter log when requeue is false or the attempt limit is reached. A delivery that is never acknowledged is handed out again when its timeout expires. `Listen` serves that broker on TCP, and `Connect` is the client. A dropped connection puts an unacknowledged delivery back on the group's queue. The next `Receive` dials again and continues from the stored offset.

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

`Receive` waits while the group already has its prefetch count in flight. The default is one, so settle the current delivery before calling `Receive` again on the same goroutine. `NewWith` changes the acknowledgement timeout, the attempt limit, and prefetch. `OpenWith` does the same for a directory and can set the segment size. Zero values keep the defaults of 30 seconds, 5 attempts, prefetch 1, and 1 MiB segments.

```go
broker, err := forgemq.Open(dir)
server, err := forgemq.Listen("127.0.0.1:9090", broker)
client, err := forgemq.Connect("127.0.0.1:9090")
err = client.CreateTopic(ctx, "orders.created")
msg, err = client.Publish(ctx, forgemq.Message{
    Topic:   "orders.created",
    Payload: []byte(`{"order_id":1001}`),
})
subscription, err := client.Subscribe(ctx, "orders.created", "accounting")
event, err := subscription.Receive(ctx)
err = event.Ack()
```

A segment is removed only after every group has moved past it. A new group then starts at the earliest offset still on disk.

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
