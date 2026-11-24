package forgemq

import "time"

// Message is one record in a topic log. Offset is assigned when the message is published.
type Message struct {
	ID        uint64
	Topic     string
	Key       []byte
	Payload   []byte
	Headers   map[string]string
	Offset    uint64
	Timestamp time.Time
}

func cloneMessage(m Message) Message {
	out := m
	out.Key = cloneBytes(m.Key)
	out.Payload = cloneBytes(m.Payload)
	if m.Headers != nil {
		out.Headers = make(map[string]string, len(m.Headers))
		for k, v := range m.Headers {
			out.Headers[k] = v
		}
	}
	return out
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
