package forgemq

import (
	"strconv"
	"time"
)

// Delivery is one in-flight copy of a log record. Ack finishes it. Nack either
// sends it again or, when requeue is false or the attempt limit is reached,
// appends it to the topic's dead-letter log.
type Delivery struct {
	Message
	topic  *topic
	group  *group
	offset uint64
	token  uint64
}

func (d *Delivery) Ack() error {
	return d.topic.finish(d.group, d.offset, d.token, true, false)
}

func (d *Delivery) Nack(requeue bool) error {
	return d.topic.finish(d.group, d.offset, d.token, false, requeue)
}

func (t *topic) finish(g *group, offset uint64, token uint64, ack bool, again bool) error {
	t.mu.Lock()
	f := g.flight[offset]
	if f == nil || f.settling || f.token != token {
		t.mu.Unlock()
		return ErrSettled
	}
	f.settling = true
	if f.timer != nil {
		f.timer.Stop()
	}
	if ack {
		t.complete(g, offset)
		t.mu.Unlock()
		return nil
	}
	if again && f.attempt < g.maxAttempts {
		delete(g.flight, offset)
		g.ready = append([]requeue{{offset: offset, attempt: f.attempt + 1}}, g.ready...)
		t.cond.Broadcast()
		t.mu.Unlock()
		return nil
	}
	reason := "attempts"
	if !again {
		reason = "rejected"
	}
	attempt := f.attempt
	msg := cloneMessage(t.log[offset])
	t.mu.Unlock()

	err := t.broker.deadLetter(t.name, msg, reason, attempt)

	t.mu.Lock()
	defer t.mu.Unlock()
	cur := g.flight[offset]
	if cur == nil {
		return err
	}
	if err != nil {
		cur.settling = false
		delete(g.flight, offset)
		g.ready = append([]requeue{{offset: offset, attempt: attempt}}, g.ready...)
		t.cond.Broadcast()
		return err
	}
	t.complete(g, offset)
	return nil
}

func (t *topic) expire(g *group, offset uint64, token uint64) {
	t.mu.Lock()
	f := g.flight[offset]
	if f == nil || f.settling || f.token != token || t.closed {
		t.mu.Unlock()
		return
	}
	f.settling = true
	if f.attempt < g.maxAttempts {
		delete(g.flight, offset)
		g.ready = append([]requeue{{offset: offset, attempt: f.attempt + 1}}, g.ready...)
		t.cond.Broadcast()
		t.mu.Unlock()
		return
	}
	attempt := f.attempt
	msg := cloneMessage(t.log[offset])
	t.mu.Unlock()

	err := t.broker.deadLetter(t.name, msg, "timeout", attempt)

	t.mu.Lock()
	defer t.mu.Unlock()
	cur := g.flight[offset]
	if cur == nil || (t.closed && err != nil) {
		return
	}
	if err != nil {
		cur.settling = false
		if !t.closed {
			cur.timer = time.AfterFunc(g.ackTimeout, func() { t.expire(g, offset, token) })
		}
		return
	}
	t.complete(g, offset)
}

func (t *topic) complete(g *group, offset uint64) {
	delete(g.flight, offset)
	g.done[offset] = true
	g.advance()
	t.cond.Broadcast()
}

func (b *Broker) deadLetter(topicName string, msg Message, reason string, attempt int) error {
	name := topicName + ".dlq"
	if err := validName(name); err != nil {
		return err
	}
	if msg.Headers == nil {
		msg.Headers = make(map[string]string)
	}
	msg.Headers["forgemq-original-topic"] = topicName
	msg.Headers["forgemq-original-offset"] = strconv.FormatUint(msg.Offset, 10)
	msg.Headers["forgemq-attempts"] = strconv.Itoa(attempt)
	msg.Headers["forgemq-reason"] = reason
	msg.Topic = name

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrClosed
	}
	dest := b.topics[name]
	if dest == nil {
		dest = newTopic(b, name)
		b.topics[name] = dest
	}
	b.nextID++
	id := b.nextID
	b.mu.Unlock()
	_, err := dest.publish(id, msg)
	return err
}
