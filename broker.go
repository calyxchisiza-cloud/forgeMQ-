package forgemq

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrTopicExists   = errors.New("topic already exists")
	ErrTopicNotFound = errors.New("topic not found")
	ErrInvalidName   = errors.New("name is not a topic identifier")
	ErrClosed        = errors.New("broker is closed")
)

// Broker is an in-memory topic log. Each subscription group keeps its own offset.
// Consumers in one group take the next message one at a time.
type Broker struct {
	mu     sync.Mutex
	topics map[string]*topic
	nextID uint64
	closed bool
}

func New() *Broker {
	return &Broker{topics: make(map[string]*topic)}
}

func (b *Broker) CreateTopic(name string) error {
	if err := validName(name); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	if _, ok := b.topics[name]; ok {
		return ErrTopicExists
	}
	b.topics[name] = newTopic(name)
	return nil
}

func (b *Broker) Publish(msg Message) (Message, error) {
	if err := validName(msg.Topic); err != nil {
		return Message{}, err
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return Message{}, ErrClosed
	}
	t := b.topics[msg.Topic]
	if t == nil {
		b.mu.Unlock()
		return Message{}, ErrTopicNotFound
	}
	b.nextID++
	id := b.nextID
	b.mu.Unlock()
	return t.publish(id, msg)
}

// Subscribe registers a consumer on a group. A new group starts at offset 0.
// Another consumer in the same group shares that group's offset.
func (b *Broker) Subscribe(topicName, groupName string) (*Consumer, error) {
	if err := validName(topicName); err != nil {
		return nil, err
	}
	if err := validName(groupName); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrClosed
	}
	t := b.topics[topicName]
	if t == nil {
		return nil, ErrTopicNotFound
	}
	return t.join(groupName), nil
}

func (b *Broker) Close() {
	b.mu.Lock()
	b.closed = true
	topics := make([]*topic, 0, len(b.topics))
	for _, t := range b.topics {
		topics = append(topics, t)
	}
	b.mu.Unlock()
	for _, t := range topics {
		t.close()
	}
}

type topic struct {
	mu     sync.Mutex
	cond   *sync.Cond
	name   string
	log    []Message
	groups map[string]*group
	closed bool
}

type group struct {
	name string
	next uint64
}

func newTopic(name string) *topic {
	t := &topic{name: name, groups: make(map[string]*group)}
	t.cond = sync.NewCond(&t.mu)
	return t
}

func (t *topic) publish(id uint64, msg Message) (Message, error) {
	stored := cloneMessage(msg)
	stored.ID = id
	stored.Topic = t.name
	stored.Timestamp = time.Now().UTC()
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return Message{}, ErrClosed
	}
	stored.Offset = uint64(len(t.log))
	t.log = append(t.log, cloneMessage(stored))
	t.cond.Broadcast()
	return cloneMessage(stored), nil
}

func (t *topic) join(name string) *Consumer {
	t.mu.Lock()
	defer t.mu.Unlock()
	g := t.groups[name]
	if g == nil {
		g = &group{name: name}
		t.groups[name] = g
	}
	return &Consumer{topic: t, group: g}
}

func (t *topic) close() {
	t.mu.Lock()
	t.closed = true
	t.cond.Broadcast()
	t.mu.Unlock()
}

// Consumer reads a topic from one group's offset.
type Consumer struct {
	topic *topic
	group *group
}

func (c *Consumer) Group() string { return c.group.name }

// Offset is the next offset this group will deliver.
func (c *Consumer) Offset() uint64 {
	c.topic.mu.Lock()
	defer c.topic.mu.Unlock()
	return c.group.next
}

// Receive returns the next message for this group. It waits until one is published,
// the context ends, or the broker closes. The group's offset advances before Receive returns.
func (c *Consumer) Receive(ctx context.Context) (Message, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	t := c.topic
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Message{}, err
	}
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			t.mu.Lock()
			t.cond.Broadcast()
			t.mu.Unlock()
		case <-done:
		}
	}()
	for c.group.next >= uint64(len(t.log)) {
		if t.closed {
			return Message{}, ErrClosed
		}
		if err := ctx.Err(); err != nil {
			return Message{}, err
		}
		t.cond.Wait()
	}
	if t.closed {
		return Message{}, ErrClosed
	}
	msg := cloneMessage(t.log[c.group.next])
	c.group.next++
	return msg, nil
}

func validName(name string) error {
	if name == "" || len(name) > 128 {
		return ErrInvalidName
	}
	for i, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
		case i > 0 && (r >= '0' && r <= '9' || r == '.' || r == '-'):
		default:
			return ErrInvalidName
		}
	}
	return nil
}
