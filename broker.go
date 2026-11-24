package forgemq

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"
)

var (
	ErrTopicExists   = errors.New("topic already exists")
	ErrTopicNotFound = errors.New("topic not found")
	ErrInvalidName   = errors.New("name is not a topic identifier")
	ErrClosed        = errors.New("broker is closed")
	ErrSettled       = errors.New("delivery is already settled")
)

// Config controls acknowledgement. Zero values use the defaults: a 30 second
// timeout, 5 attempts, and one in-flight message per group.
type Config struct {
	AckTimeout  time.Duration
	MaxAttempts int
	Prefetch    int
}

// Broker is an in-memory topic log. Each subscription group keeps its own offset.
// Consumers in one group share that group's in-flight messages.
type Broker struct {
	mu     sync.Mutex
	topics map[string]*topic
	nextID uint64
	closed bool
	cfg    Config
}

func New() *Broker {
	return NewWith(Config{})
}

func NewWith(cfg Config) *Broker {
	if cfg.AckTimeout <= 0 {
		cfg.AckTimeout = 30 * time.Second
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.Prefetch <= 0 {
		cfg.Prefetch = 1
	}
	return &Broker{topics: make(map[string]*topic), cfg: cfg}
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
	b.topics[name] = newTopic(b, name)
	return nil
}

// Publish appends the message and returns it with its id and offset.
// That return is the confirmation that the broker accepted the message.
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
	broker    *Broker
	mu        sync.Mutex
	cond      *sync.Cond
	name      string
	log       []Message
	groups    map[string]*group
	nextToken uint64
	closed    bool
}

type requeue struct {
	offset  uint64
	attempt int
}

type flight struct {
	offset   uint64
	attempt  int
	token    uint64
	timer    *time.Timer
	settling bool
}

type group struct {
	name        string
	commit      uint64
	next        uint64
	ready       []requeue
	flight      map[uint64]*flight
	done        map[uint64]bool
	prefetch    int
	ackTimeout  time.Duration
	maxAttempts int
}

func newTopic(b *Broker, name string) *topic {
	t := &topic{broker: b, name: name, groups: make(map[string]*group)}
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
		cfg := t.broker.cfg
		g = &group{
			name:        name,
			flight:      make(map[uint64]*flight),
			done:        make(map[uint64]bool),
			prefetch:    cfg.Prefetch,
			ackTimeout:  cfg.AckTimeout,
			maxAttempts: cfg.MaxAttempts,
		}
		t.groups[name] = g
	}
	return &Consumer{topic: t, group: g}
}

func (t *topic) close() {
	t.mu.Lock()
	t.closed = true
	for _, g := range t.groups {
		for _, f := range g.flight {
			if f.timer != nil {
				f.timer.Stop()
			}
		}
	}
	t.cond.Broadcast()
	t.mu.Unlock()
}

// Consumer reads a topic from one group's offset.
type Consumer struct {
	topic *topic
	group *group
}

func (c *Consumer) Group() string { return c.group.name }

// Offset is the next offset this group has not finished. Ack and dead-letter move it.
func (c *Consumer) Offset() uint64 {
	c.topic.mu.Lock()
	defer c.topic.mu.Unlock()
	return c.group.commit
}

// Receive waits for the next message this group has not finished. The offset stays
// put until Ack. A second call waits when the group already has Prefetch deliveries in flight.
func (c *Consumer) Receive(ctx context.Context) (*Delivery, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	t := c.topic
	g := c.group
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
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
	for {
		if t.closed {
			return nil, ErrClosed
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(g.flight) >= g.prefetch {
			t.cond.Wait()
			continue
		}
		var offset uint64
		var attempt int
		switch {
		case len(g.ready) > 0:
			item := g.ready[0]
			g.ready = g.ready[1:]
			offset, attempt = item.offset, item.attempt
		case g.next < uint64(len(t.log)):
			offset = g.next
			attempt = 1
			g.next++
		default:
			t.cond.Wait()
			continue
		}
		t.nextToken++
		f := &flight{offset: offset, attempt: attempt, token: t.nextToken}
		g.flight[offset] = f
		token := f.token
		f.timer = time.AfterFunc(g.ackTimeout, func() { t.expire(g, offset, token) })
		return &Delivery{Message: t.messageFor(offset, attempt), topic: t, group: g, offset: offset, token: token}, nil
	}
}

func (t *topic) messageFor(offset uint64, attempt int) Message {
	msg := cloneMessage(t.log[offset])
	if attempt > 1 {
		if msg.Headers == nil {
			msg.Headers = make(map[string]string)
		}
		msg.Headers["forgemq-attempt"] = strconv.Itoa(attempt)
	}
	return msg
}

func (g *group) advance() {
	for g.done[g.commit] {
		delete(g.done, g.commit)
		g.commit++
	}
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
