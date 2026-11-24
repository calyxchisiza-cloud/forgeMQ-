package forgemq

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

// Options selects a token and TLS for ConnectWith.
// An empty token speaks to a broker that has no principals.
type Options struct {
	Token string
	TLS   *tls.Config
}

// Client publishes and subscribes over a TCP connection.
// A dropped connection is dialed again on the next call.
// Receive keeps waiting through one reconnect and then continues from the group's stored offset.
type Client struct {
	addr     string
	mu       sync.Mutex
	conn     net.Conn
	wmu      sync.Mutex
	dead     bool
	closed   bool
	gen      uint64
	nextCorr uint32
	pending  map[uint32]chan rpcResult
	subs     map[uint32]*Subscription
	dialWait chan struct{}
	token    string
	tls      *tls.Config
}

type rpcResult struct {
	body []byte
	err  error
}

// Subscription is one group's view of a topic on a client connection.
type Subscription struct {
	client *Client
	topic  string
	group  string
	id     uint32
	gen    uint64
}

// Event is one delivery received from the server. Ack and Nack settle it.
// A second call, or a call after the connection dropped, returns ErrSettled.
type Event struct {
	Message
	client *Client
	subID  uint32
	token  uint64
	gen    uint64
}

// Connect dials addr. The dial gives up after 5 seconds.
func Connect(addr string) (*Client, error) {
	return ConnectWith(addr, Options{})
}

// ConnectWith dials addr, presents opt.Token when it is set, and uses TLS when opt.TLS is set.
func ConnectWith(addr string, opt Options) (*Client, error) {
	if addr == "" {
		return nil, errors.New("address is required")
	}
	c := &Client{
		addr:    addr,
		dead:    true,
		pending: make(map[uint32]chan rpcResult),
		subs:    make(map[uint32]*Subscription),
		token:   opt.Token,
		tls:     opt.TLS,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.reconnect(ctx); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	c.closed = true
	c.dead = true
	conn := c.conn
	c.mu.Unlock()
	if conn != nil {
		return conn.Close()
	}
	return nil
}

func (c *Client) CreateTopic(ctx context.Context, name string) error {
	var buf bytes.Buffer
	if err := putString(&buf, name); err != nil {
		return err
	}
	body, err := c.call(orBackground(ctx), opCreateTopic, buf.Bytes(), true)
	if err != nil {
		return err
	}
	code, rest, err := splitStatus(body)
	if err != nil {
		return err
	}
	if len(rest) != 0 {
		return ErrBadRequest
	}
	return statusError(code)
}

func (c *Client) Publish(ctx context.Context, msg Message) (Message, error) {
	var buf bytes.Buffer
	if err := appendFields(&buf, msg); err != nil {
		return Message{}, err
	}
	body, err := c.call(orBackground(ctx), opPublish, buf.Bytes(), true)
	if err != nil {
		return Message{}, err
	}
	code, rest, err := splitStatus(body)
	if err != nil {
		return Message{}, err
	}
	if err := statusError(code); err != nil {
		return Message{}, err
	}
	if len(rest) != 24 {
		return Message{}, ErrBadRequest
	}
	out := cloneMessage(msg)
	out.ID = binary.BigEndian.Uint64(rest[0:8])
	out.Offset = binary.BigEndian.Uint64(rest[8:16])
	out.Timestamp = time.Unix(0, int64(binary.BigEndian.Uint64(rest[16:24]))).UTC()
	return out, nil
}

func (c *Client) PublishBatch(ctx context.Context, msgs []Message) ([]Message, error) {
	if len(msgs) == 0 {
		return nil, nil
	}
	if len(msgs) > maxBatch {
		return nil, ErrTooLarge
	}
	var buf bytes.Buffer
	putU32(&buf, uint32(len(msgs)))
	for _, msg := range msgs {
		if err := appendFields(&buf, msg); err != nil {
			return nil, err
		}
	}
	body, err := c.call(orBackground(ctx), opPublishBatch, buf.Bytes(), true)
	if err != nil {
		return nil, err
	}
	code, rest, err := splitStatus(body)
	if err != nil {
		return nil, err
	}
	if err := statusError(code); err != nil {
		return nil, err
	}
	if len(rest) != len(msgs)*24 {
		return nil, ErrBadRequest
	}
	out := make([]Message, len(msgs))
	for i := range msgs {
		out[i] = cloneMessage(msgs[i])
		base := i * 24
		out[i].ID = binary.BigEndian.Uint64(rest[base : base+8])
		out[i].Offset = binary.BigEndian.Uint64(rest[base+8 : base+16])
		out[i].Timestamp = time.Unix(0, int64(binary.BigEndian.Uint64(rest[base+16:base+24]))).UTC()
	}
	return out, nil
}

func (c *Client) Subscribe(ctx context.Context, topic, group string) (*Subscription, error) {
	s := &Subscription{client: c, topic: topic, group: group}
	if err := c.attach(orBackground(ctx), s); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Subscription) Receive(ctx context.Context) (*Event, error) {
	ctx = orBackground(ctx)
	revived := false
	for {
		s.client.mu.Lock()
		if s.client.closed {
			s.client.mu.Unlock()
			return nil, ErrClosed
		}
		id := s.id
		s.client.mu.Unlock()
		if id == 0 {
			if revived {
				return nil, ErrClosed
			}
			revived = true
			if err := s.revive(ctx); err != nil {
				return nil, err
			}
			continue
		}
		var buf bytes.Buffer
		putU32(&buf, id)
		body, err := s.client.call(ctx, opReceive, buf.Bytes(), false)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if errors.Is(err, ErrClosed) {
				return nil, err
			}
			if revived {
				return nil, err
			}
			revived = true
			if err := s.revive(ctx); err != nil {
				return nil, err
			}
			continue
		}
		code, rest, err := splitStatus(body)
		if err != nil {
			return nil, err
		}
		if err := statusError(code); err != nil {
			if errors.Is(err, ErrBadRequest) && !revived {
				revived = true
				s.client.mu.Lock()
				s.id = 0
				s.client.mu.Unlock()
				if err := s.revive(ctx); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		ev, err := decodeDeliver(rest)
		if err != nil {
			return nil, err
		}
		s.client.mu.Lock()
		ev.client = s.client
		ev.gen = s.client.gen
		ev.subID = s.id
		s.client.mu.Unlock()
		return ev, nil
	}
}

func (e *Event) Ack() error {
	return e.finish(opAck, 0, false)
}

func (e *Event) Nack(requeue bool) error {
	flag := byte(0)
	if requeue {
		flag = 1
	}
	return e.finish(opNack, flag, true)
}

func (e *Event) Poison() error {
	return e.finish(opPoison, 0, false)
}

func (e *Event) finish(op uint16, flag byte, nack bool) error {
	e.client.mu.Lock()
	if e.client.closed {
		e.client.mu.Unlock()
		return ErrClosed
	}
	if e.client.dead || e.client.gen != e.gen {
		e.client.mu.Unlock()
		return ErrSettled
	}
	e.client.mu.Unlock()
	var buf bytes.Buffer
	putU32(&buf, e.subID)
	putU64(&buf, e.token)
	if nack {
		buf.WriteByte(flag)
	}
	body, err := e.client.call(context.Background(), op, buf.Bytes(), false)
	if err != nil {
		if errors.Is(err, ErrClosed) {
			return err
		}
		return ErrSettled
	}
	code, rest, err := splitStatus(body)
	if err != nil {
		return err
	}
	if len(rest) != 0 {
		return ErrBadRequest
	}
	return statusError(code)
}

func (s *Subscription) revive(ctx context.Context) error {
	if err := s.client.reconnect(ctx); err != nil {
		return err
	}
	return s.client.attach(ctx, s)
}

func (c *Client) attach(ctx context.Context, s *Subscription) error {
	var buf bytes.Buffer
	if err := putString(&buf, s.topic); err != nil {
		return err
	}
	if err := putString(&buf, s.group); err != nil {
		return err
	}
	body, err := c.call(ctx, opSubscribe, buf.Bytes(), true)
	if err != nil {
		return err
	}
	code, rest, err := splitStatus(body)
	if err != nil {
		return err
	}
	if err := statusError(code); err != nil {
		return err
	}
	if len(rest) != 4 {
		return ErrBadRequest
	}
	id := binary.BigEndian.Uint32(rest)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.dead {
		return ErrClosed
	}
	if s.id != 0 {
		delete(c.subs, s.id)
	}
	s.id = id
	s.gen = c.gen
	c.subs[id] = s
	return nil
}

func (c *Client) call(ctx context.Context, op uint16, payload []byte, redial bool) ([]byte, error) {
	if redial {
		if err := c.ensure(ctx); err != nil {
			return nil, err
		}
	}
	ch := make(chan rpcResult, 1)
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	if c.dead || c.conn == nil {
		c.mu.Unlock()
		return nil, io.ErrClosedPipe
	}
	c.nextCorr++
	if c.nextCorr == 0 {
		c.nextCorr = 1
	}
	corr := c.nextCorr
	c.pending[corr] = ch
	c.mu.Unlock()
	if err := c.write(corr, op, payload); err != nil {
		c.mu.Lock()
		delete(c.pending, corr)
		c.mu.Unlock()
		c.breakConn()
		return nil, err
	}
	select {
	case res := <-ch:
		if res.err != nil {
			return nil, res.err
		}
		return res.body, nil
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, corr)
		c.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (c *Client) ensure(ctx context.Context) error {
	c.mu.Lock()
	closed := c.closed
	ready := !c.closed && !c.dead && c.conn != nil
	c.mu.Unlock()
	if closed {
		return ErrClosed
	}
	if ready {
		return nil
	}
	return c.reconnect(ctx)
}

func (c *Client) reconnect(ctx context.Context) error {
	ctx = orBackground(ctx)
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return ErrClosed
		}
		if !c.dead && c.conn != nil {
			c.mu.Unlock()
			return nil
		}
		if c.dialWait != nil {
			ch := c.dialWait
			c.mu.Unlock()
			select {
			case <-ch:
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		ch := make(chan struct{})
		c.dialWait = ch
		c.mu.Unlock()
		err := c.dial(ctx)
		if err == nil && c.token != "" {
			err = c.authenticate(ctx)
			if err != nil {
				c.breakConn()
			}
		}
		c.mu.Lock()
		c.dialWait = nil
		close(ch)
		c.mu.Unlock()
		return err
	}
}

func (c *Client) authenticate(ctx context.Context) error {
	var buf bytes.Buffer
	if err := putString(&buf, c.token); err != nil {
		return err
	}
	body, err := c.call(ctx, opAuth, buf.Bytes(), false)
	if err != nil {
		return err
	}
	code, rest, err := splitStatus(body)
	if err != nil {
		return err
	}
	if len(rest) != 0 {
		return ErrBadRequest
	}
	return statusError(code)
}

func (c *Client) dial(ctx context.Context) error {
	var conn net.Conn
	var err error
	if c.tls != nil {
		d := tls.Dialer{Config: c.tls}
		conn, err = d.DialContext(ctx, "tcp", c.addr)
	} else {
		var d net.Dialer
		conn, err = d.DialContext(ctx, "tcp", c.addr)
	}
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		conn.Close()
		return ErrClosed
	}
	if c.conn != nil {
		c.conn.Close()
	}
	c.gen++
	gen := c.gen
	c.conn = conn
	c.dead = false
	c.pending = make(map[uint32]chan rpcResult)
	for _, sub := range c.subs {
		sub.id = 0
	}
	c.subs = make(map[uint32]*Subscription)
	go c.readLoop(conn, gen)
	return nil
}

func (c *Client) readLoop(conn net.Conn, gen uint64) {
	defer c.failConn(conn, gen)
	r := bufio.NewReader(conn)
	for {
		corr, _, payload, err := readFrame(r)
		if err != nil {
			return
		}
		c.mu.Lock()
		ch := c.pending[corr]
		delete(c.pending, corr)
		c.mu.Unlock()
		if ch != nil {
			offer(ch, rpcResult{body: payload})
		}
	}
}

func decodeDeliver(payload []byte) (*Event, error) {
	if len(payload) < 4+8+8+8+8 {
		return nil, ErrBadRequest
	}
	ev := &Event{
		subID: binary.BigEndian.Uint32(payload[0:4]),
		token: binary.BigEndian.Uint64(payload[4:12]),
	}
	ev.ID = binary.BigEndian.Uint64(payload[12:20])
	ev.Offset = binary.BigEndian.Uint64(payload[20:28])
	ev.Timestamp = time.Unix(0, int64(binary.BigEndian.Uint64(payload[28:36]))).UTC()
	r := sliceReader{b: payload[36:]}
	msg, err := readFields(&r)
	if err != nil || !finished(r) {
		return nil, ErrBadRequest
	}
	ev.Topic = msg.Topic
	ev.Key = msg.Key
	ev.Payload = msg.Payload
	ev.Headers = msg.Headers
	return ev, nil
}

func (c *Client) failConn(conn net.Conn, gen uint64) {
	c.mu.Lock()
	if c.gen != gen || c.conn != conn {
		c.mu.Unlock()
		return
	}
	c.dead = true
	pending := c.pending
	c.pending = make(map[uint32]chan rpcResult)
	c.mu.Unlock()
	conn.Close()
	for _, ch := range pending {
		offer(ch, rpcResult{err: io.ErrUnexpectedEOF})
	}
}

func (c *Client) write(corr uint32, op uint16, payload []byte) error {
	buf, err := frameBytes(corr, op, payload)
	if err != nil {
		return err
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	c.mu.Lock()
	conn := c.conn
	dead := c.dead || c.closed || conn == nil
	c.mu.Unlock()
	if dead {
		return io.ErrClosedPipe
	}
	_, err = conn.Write(buf)
	return err
}

func (c *Client) breakConn() {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn != nil {
		conn.Close()
	}
}

func offer(ch chan rpcResult, res rpcResult) {
	select {
	case ch <- res:
	default:
	}
}

func orBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
