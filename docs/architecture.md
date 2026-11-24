# Architecture

ForgeMQ v1, so far, is one process and one in-memory log per topic. A published message is appended and given the next offset, starting at 0. The record keeps an id, the topic, an optional key, headers, the payload, and the time it was accepted.

A subscription group is a name plus the next offset that group will read. Creating the group starts that offset at 0, including when the log already holds messages. Each group therefore receives every message. `Receive` waits when the group has caught up, then returns one message and advances that group's offset. Two consumers in the same group share the offset, so one message is handed to one of them.

`Close` makes a waiting `Receive` return. Publishing to a missing topic fails. Creating a topic twice fails.

Not built yet: acknowledgements, redelivery, dead-letter topics, publisher confirmations, prefetch, disk segments, retention, TCP, authentication, and more than one broker process.
