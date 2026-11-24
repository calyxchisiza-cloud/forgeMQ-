# Architecture

ForgeMQ v1, so far, is one process and one in-memory log per topic. A published message is appended and given the next offset, starting at 0. The record keeps an id, the topic, an optional key, headers, the payload, and the time it was accepted.

A subscription group is a name plus the next offset that group has not finished. Creating the group starts that offset at 0, including when the log already holds messages. Each group therefore receives every message. `Receive` waits when the group has caught up, or when it already holds its prefetch count of unacknowledged deliveries. The default prefetch is one. Two consumers in the same group share that in-flight set, so one message is handed to one of them.

`Ack` finishes a delivery. The group's offset moves forward only through a contiguous run of finished offsets, so an earlier delivery can be acknowledged after a later one when prefetch is greater than one. `Nack(true)` puts the same log offset at the front of the group's queue and counts another attempt. `Nack(false)` appends the record to `<topic>.dlq` immediately. The attempt that reaches the limit is appended there instead of being queued again. A timer does the same as `Nack(true)` when the delivery is still outstanding. The dead-letter record keeps the key, payload, and headers, and adds `forgemq-original-topic`, `forgemq-original-offset`, `forgemq-attempts`, and `forgemq-reason`. A second `Ack` or `Nack` returns `ErrSettled`.

The message returned by `Publish` is the confirmation that the record is in the log. `Close` makes a waiting `Receive` return. Publishing to a missing topic fails. Creating a topic twice fails.

Not built yet: disk segments, retention, durable offsets, TCP, authentication, and more than one broker process.
