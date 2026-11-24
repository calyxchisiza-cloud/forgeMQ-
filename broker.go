package forgemq

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrTopicExists   = errors.New("topic already exists")
	ErrTopicNotFound = errors.New("topic not found")
	ErrInvalidName   = errors.New("name is not a topic identifier")
	ErrClosed        = errors.New("broker is closed")
	ErrSettled       = errors.New("delivery is already settled")
	ErrCorrupt       = errors.New("log is corrupt")
	ErrTooLarge      = errors.New("message is too large")
	ErrBadRequest    = errors.New("request is not valid")
	ErrDenied        = errors.New("not allowed")
	ErrLimited       = errors.New("resource limit reached")
	ErrFault         = errors.New("injected fault")
)

// Principal is a client token and the commands it may send.
// Publish covers creating a topic and appending a record.
// Consume covers subscribe, receive, and settling a delivery.
type Principal struct {
	Token   string
	Publish bool
	Consume bool
}

// Config controls acknowledgement and, for Open, where the log is stored.
// Zero values use the defaults: a 30 second timeout, 5 attempts, one in-flight
// message per group, and 1 MiB segments.
type Config struct {
	AckTimeout       time.Duration
	MaxAttempts      int
	Prefetch         int
	Dir              string
	SegmentBytes     int
	Principals       []Principal
	MaxConnections   int
	MaxSubscriptions int
	MaxTopics        int
	Logger           *slog.Logger
}

// Broker is a topic log. New keeps it in memory. Open stores it in a directory
// and reloads it after a restart. Consumers in one group share that group's
// in-flight messages.
type Broker struct {
	mu           sync.Mutex
	topics       map[string]*topic
	nextID       uint64
	closed       bool
	cfg          Config
	dir          string
	segmentBytes int64
	commits      *commitLog
	principals   []Principal
	logger       *slog.Logger
	faults       atomic.Int64
	published    atomic.Uint64
	delivered    atomic.Uint64
	acked        atomic.Uint64
	nacked       atomic.Uint64
	dead         atomic.Uint64
	authFailures atomic.Uint64
	limited      atomic.Uint64
	connections  atomic.Int64
	faultCount   atomic.Uint64
}

func New() *Broker {
	return NewWith(Config{})
}

func NewWith(cfg Config) *Broker {
	if cfg.Dir != "" {
		panic("forgemq.NewWith keeps the log in memory; use Open")
	}
	b, err := openBroker(cfg)
	if err != nil {
		panic(err)
	}
	return b
}

// Open loads a broker from dir, creating the directory when it is missing.
func Open(dir string) (*Broker, error) {
	return OpenWith(Config{Dir: dir})
}

// OpenWith loads a broker from cfg.Dir.
func OpenWith(cfg Config) (*Broker, error) {
	if cfg.Dir == "" {
		return nil, errors.New("data directory is required")
	}
	return openBroker(cfg)
}

func openBroker(cfg Config) (*Broker, error) {
	cfg = normalize(cfg)
	b := &Broker{
		topics:       make(map[string]*topic),
		cfg:          cfg,
		dir:          cfg.Dir,
		segmentBytes: int64(cfg.SegmentBytes),
		principals:   append([]Principal(nil), cfg.Principals...),
		logger:       cfg.Logger,
	}
	if cfg.Dir == "" {
		return b, nil
	}
	if err := b.load(); err != nil {
		b.Close()
		return nil, err
	}
	return b, nil
}

func normalize(cfg Config) Config {
	if cfg.AckTimeout <= 0 {
		cfg.AckTimeout = 30 * time.Second
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.Prefetch <= 0 {
		cfg.Prefetch = 1
	}
	if cfg.SegmentBytes <= 0 {
		cfg.SegmentBytes = 1 << 20
	}
	if cfg.MaxConnections <= 0 {
		cfg.MaxConnections = 128
	}
	if cfg.MaxSubscriptions <= 0 {
		cfg.MaxSubscriptions = 64
	}
	if cfg.MaxTopics <= 0 {
		cfg.MaxTopics = 1024
	}
	return cfg
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
	if len(b.topics) >= b.cfg.MaxTopics {
		b.hitLimit("topics")
		return ErrLimited
	}
	t := newTopic(b, name)
	if err := t.prepare(); err != nil {
		t.closeFiles()
		return err
	}
	b.topics[name] = t
	return nil
}

// Publish appends the message and returns it with its id and offset.
// That return is the confirmation that the broker accepted the message.
// Open syncs the record to disk before Publish returns.
func (b *Broker) Publish(msg Message) (Message, error) {
	if err := validName(msg.Topic); err != nil {
		return Message{}, err
	}
	if b.takeFault() {
		return Message{}, ErrFault
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
	out, err := t.publish(id, msg)
	if err != nil {
		return Message{}, err
	}
	b.published.Add(1)
	return out, nil
}

// PublishBatch appends every message to one topic and syncs once per segment.
// The returned messages carry ids and offsets in order. An empty batch stores nothing.
func (b *Broker) PublishBatch(msgs []Message) ([]Message, error) {
	if len(msgs) == 0 {
		return nil, nil
	}
	if len(msgs) > maxBatch {
		return nil, ErrTooLarge
	}
	topicName := msgs[0].Topic
	if err := validName(topicName); err != nil {
		return nil, err
	}
	for i := range msgs {
		if msgs[i].Topic != topicName {
			return nil, ErrBadRequest
		}
	}
	if b.takeFault() {
		return nil, ErrFault
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, ErrClosed
	}
	t := b.topics[topicName]
	if t == nil {
		b.mu.Unlock()
		return nil, ErrTopicNotFound
	}
	ids := make([]uint64, len(msgs))
	for i := range msgs {
		b.nextID++
		ids[i] = b.nextID
	}
	b.mu.Unlock()
	out, err := t.publishMany(ids, msgs)
	if len(out) > 0 {
		b.published.Add(uint64(len(out)))
	}
	return out, err
}

// Subscribe registers a consumer on a group. A new group starts at the earliest
// offset still in the log. Another consumer in the same group shares that offset.
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
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	topics := make([]*topic, 0, len(b.topics))
	for _, t := range b.topics {
		topics = append(topics, t)
	}
	b.mu.Unlock()
	for _, t := range topics {
		t.close()
	}
	if b.commits != nil {
		b.commits.close()
	}
}

type topic struct {
	broker    *Broker
	mu        sync.Mutex
	cond      *sync.Cond
	name      string
	dir       string
	log       []Message
	base      uint64
	segs      []*segFile
	active    *segFile
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
	if b.dir != "" {
		t.dir = filepath.Join(b.dir, "data", name)
	}
	return t
}

func (t *topic) publish(id uint64, msg Message) (Message, error) {
	out, err := t.publishMany([]uint64{id}, []Message{msg})
	if err != nil {
		return Message{}, err
	}
	return out[0], nil
}

func (t *topic) publishMany(ids []uint64, msgs []Message) ([]Message, error) {
	now := time.Now().UTC()
	stored := make([]Message, len(msgs))
	for i := range msgs {
		stored[i] = cloneMessage(msgs[i])
		stored[i].ID = ids[i]
		stored[i].Topic = t.name
		stored[i].Timestamp = now
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, ErrClosed
	}
	end := t.end()
	for i := range stored {
		stored[i].Offset = end + uint64(i)
	}
	synced := len(stored)
	var err error
	if t.broker.dir != "" {
		synced, err = t.writeRecords(stored)
	}
	if synced > 0 {
		for i := 0; i < synced; i++ {
			t.log = append(t.log, cloneMessage(stored[i]))
		}
		t.cond.Broadcast()
	}
	out := make([]Message, synced)
	for i := 0; i < synced; i++ {
		out[i] = cloneMessage(stored[i])
	}
	return out, err
}

func (t *topic) join(name string) *Consumer {
	t.mu.Lock()
	defer t.mu.Unlock()
	g := t.groups[name]
	if g == nil {
		cfg := t.broker.cfg
		g = &group{
			name:        name,
			commit:      t.base,
			next:        t.base,
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
	t.closeFiles()
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
	got, err := c.ReceiveN(ctx, 1)
	if err != nil {
		return nil, err
	}
	return got[0], nil
}

// ReceiveN returns up to n deliveries. It waits for the first one, then takes
// further records that are already in the log without passing the group's prefetch.
func (c *Consumer) ReceiveN(ctx context.Context, n int) ([]*Delivery, error) {
	if n <= 0 {
		return nil, ErrBadRequest
	}
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
	var out []*Delivery
	for len(out) < n {
		if t.closed {
			if len(out) > 0 {
				return out, nil
			}
			return nil, ErrClosed
		}
		if err := ctx.Err(); err != nil {
			if len(out) > 0 {
				return out, nil
			}
			return nil, err
		}
		if len(g.flight) >= g.prefetch {
			if len(out) > 0 {
				return out, nil
			}
			t.cond.Wait()
			continue
		}
		offset, attempt, ok := t.nextDelivery(g)
		if !ok {
			if len(out) > 0 {
				return out, nil
			}
			t.cond.Wait()
			continue
		}
		t.nextToken++
		f := &flight{offset: offset, attempt: attempt, token: t.nextToken}
		g.flight[offset] = f
		token := f.token
		f.timer = time.AfterFunc(g.ackTimeout, func() { t.expire(g, offset, token) })
		t.broker.delivered.Add(1)
		out = append(out, &Delivery{
			Message: t.messageFor(offset, attempt),
			topic:   t,
			group:   g,
			offset:  offset,
			token:   token,
		})
	}
	return out, nil
}

func (t *topic) nextDelivery(g *group) (uint64, int, bool) {
	if len(g.ready) > 0 {
		item := g.ready[0]
		g.ready = g.ready[1:]
		return item.offset, item.attempt, true
	}
	if g.next < t.end() {
		offset := g.next
		g.next++
		return offset, 1, true
	}
	return 0, 0, false
}

func (t *topic) end() uint64 {
	return t.base + uint64(len(t.log))
}

func (t *topic) at(offset uint64) Message {
	return t.log[offset-t.base]
}

func (t *topic) messageFor(offset uint64, attempt int) Message {
	msg := cloneMessage(t.at(offset))
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
