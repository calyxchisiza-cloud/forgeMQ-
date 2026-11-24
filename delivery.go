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
	return d.topic.finish(d.group, d.offset, d.token, true, false, "")
}

func (d *Delivery) Nack(requeue bool) error {
	reason := "rejected"
	if requeue {
		reason = "attempts"
	}
	return d.topic.finish(d.group, d.offset, d.token, false, requeue, reason)
}

// Poison appends the message to the topic's dead-letter log immediately.
// The reason is "poison". The group's other deliveries are left where they are.
func (d *Delivery) Poison() error {
	return d.topic.finish(d.group, d.offset, d.token, false, false, "poison")
}

func (t *topic) finish(g *group, offset uint64, token uint64, ack bool, again bool, reason string) error {
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
		err := t.complete(g, offset)
		if err != nil {
			t.giveBack(g, offset, f.attempt)
		}
		t.mu.Unlock()
		if err == nil {
			t.broker.acked.Add(1)
		}
		return err
	}
	if again && f.attempt < g.maxAttempts {
		delete(g.flight, offset)
		g.ready = append([]requeue{{offset: offset, attempt: f.attempt + 1}}, g.ready...)
		t.cond.Broadcast()
		t.mu.Unlock()
		t.broker.nacked.Add(1)
		return nil
	}
	if reason == "" {
		reason = "rejected"
	}
	attempt := f.attempt
	msg := cloneMessage(t.at(offset))
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
	if err := t.complete(g, offset); err != nil {
		t.giveBack(g, offset, attempt)
		return err
	}
	if reason != "poison" {
		t.broker.nacked.Add(1)
	}
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
	msg := cloneMessage(t.at(offset))
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
	if err := t.complete(g, offset); err != nil {
		t.giveBack(g, offset, cur.attempt)
	}
}

func (t *topic) giveBack(g *group, offset uint64, attempt int) {
	if _, ok := g.flight[offset]; !ok {
		return
	}
	delete(g.flight, offset)
	g.ready = append([]requeue{{offset: offset, attempt: attempt}}, g.ready...)
	t.cond.Broadcast()
}

func (t *topic) complete(g *group, offset uint64) error {
	g.done[offset] = true
	newCommit := g.commit
	for g.done[newCommit] {
		newCommit++
	}
	if newCommit != g.commit {
		if err := t.broker.appendCommit(t.name, g.name, newCommit); err != nil {
			delete(g.done, offset)
			return err
		}
	}
	delete(g.flight, offset)
	g.advance()
	t.retain()
	t.cond.Broadcast()
	return nil
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
		if err := dest.prepare(); err != nil {
			dest.closeFiles()
			b.mu.Unlock()
			return err
		}
		b.topics[name] = dest
	}
	b.nextID++
	id := b.nextID
	b.mu.Unlock()
	_, err := dest.publish(id, msg)
	if err != nil {
		return err
	}
	b.dead.Add(1)
	b.logInfo("dead letter", "topic", topicName, "offset", msg.Offset, "reason", reason, "attempts", attempt)
	return nil
}
