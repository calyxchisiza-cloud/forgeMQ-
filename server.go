package forgemq

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"net"
	"sync"
)

// Server accepts client connections for one broker.
type Server struct {
	ln     net.Listener
	b      *Broker
	mu     sync.Mutex
	conns  map[*wireConn]struct{}
	closed bool
	wg     sync.WaitGroup
}

// Listen serves the broker at addr. The returned server is already accepting.
func Listen(addr string, b *Broker) (*Server, error) {
	if b == nil {
		return nil, errors.New("broker is required")
	}
	if addr == "" {
		return nil, errors.New("address is required")
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return serve(ln, b)
}

// ListenTLS serves the broker with the same frames inside a TLS connection.
func ListenTLS(addr string, b *Broker, cfg *tls.Config) (*Server, error) {
	if b == nil {
		return nil, errors.New("broker is required")
	}
	if addr == "" {
		return nil, errors.New("address is required")
	}
	if cfg == nil {
		return nil, errors.New("tls config is required")
	}
	ln, err := tls.Listen("tcp", addr, cfg)
	if err != nil {
		return nil, err
	}
	return serve(ln, b)
}

func serve(ln net.Listener, b *Broker) (*Server, error) {
	s := &Server{ln: ln, b: b, conns: make(map[*wireConn]struct{})}
	s.wg.Add(1)
	go s.accept()
	return s, nil
}

func (s *Server) Addr() string {
	return s.ln.Addr().String()
}

func (s *Server) Close() error {
	s.mu.Lock()
	s.closed = true
	conns := make([]*wireConn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	err := s.ln.Close()
	for _, c := range conns {
		c.cleanup()
	}
	s.wg.Wait()
	return err
}

func (s *Server) accept() {
	defer s.wg.Done()
	for {
		nc, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			nc.Close()
			return
		}
		if len(s.conns) >= s.b.cfg.MaxConnections {
			s.mu.Unlock()
			nc.Close()
			s.b.hitLimit("connections")
			continue
		}
		w := newWire(s, nc)
		s.conns[w] = struct{}{}
		s.b.connections.Add(1)
		s.mu.Unlock()
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() {
				s.mu.Lock()
				delete(s.conns, w)
				s.mu.Unlock()
			}()
			defer w.cleanup()
			w.readLoop()
		}()
	}
}

type tracked struct {
	d   *Delivery
	sub uint32
}

type srvSub struct {
	id       uint32
	consumer *Consumer
	ctx      context.Context
	cancel   context.CancelFunc
}

type wireConn struct {
	s        *Server
	c        net.Conn
	once     sync.Once
	mu       sync.Mutex
	wmu      sync.Mutex
	wg       sync.WaitGroup
	closed   bool
	subs     map[uint32]*srvSub
	inflight map[uint64]tracked
	nextSub  uint32
	nextTok  uint64
	authed   bool
	canPub   bool
	canCons  bool
}

func newWire(s *Server, c net.Conn) *wireConn {
	return &wireConn{
		s:        s,
		c:        c,
		subs:     make(map[uint32]*srvSub),
		inflight: make(map[uint64]tracked),
	}
}

func (w *wireConn) readLoop() {
	r := bufio.NewReader(w.c)
	for {
		corr, op, payload, err := readFrame(r)
		if err != nil {
			return
		}
		if corr == 0 || op == opDeliver {
			return
		}
		if op == opReceive {
			w.startReceive(corr, payload)
			continue
		}
		if err := w.writeFrame(corr, op, w.handle(op, payload)); err != nil {
			return
		}
	}
}

func (w *wireConn) handle(op uint16, payload []byte) []byte {
	if op == opAuth {
		return w.onAuth(payload)
	}
	consume := op != opCreateTopic && op != opPublish && op != opPublishBatch
	if err := w.permit(consume); err != nil {
		return statusBody(err)
	}
	switch op {
	case opCreateTopic:
		return w.onCreate(payload)
	case opPublish:
		return w.onPublish(payload)
	case opPublishBatch:
		return w.onPublishBatch(payload)
	case opSubscribe:
		return w.onSubscribe(payload)
	case opAck:
		return w.onSettle(payload, true)
	case opNack:
		return w.onSettle(payload, false)
	case opPoison:
		return w.onPoison(payload)
	default:
		return statusBody(ErrBadRequest)
	}
}

func (w *wireConn) permit(consume bool) error {
	if !w.s.b.secured() {
		return nil
	}
	w.mu.Lock()
	authed := w.authed
	ok := w.canPub
	if consume {
		ok = w.canCons
	}
	w.mu.Unlock()
	if !authed || !ok {
		w.s.b.deny()
		return ErrDenied
	}
	return nil
}

func (w *wireConn) onAuth(payload []byte) []byte {
	r := sliceReader{b: payload}
	token, err := r.str()
	if err != nil || !finished(r) {
		return statusBody(ErrBadRequest)
	}
	publish, consume, err := w.s.b.principal(token)
	if err != nil {
		w.s.b.deny()
		return statusBody(err)
	}
	w.mu.Lock()
	w.authed = true
	w.canPub = publish
	w.canCons = consume
	w.mu.Unlock()
	return statusBody(nil)
}

func (w *wireConn) onCreate(payload []byte) []byte {
	r := sliceReader{b: payload}
	name, err := r.str()
	if err != nil || !finished(r) {
		return statusBody(ErrBadRequest)
	}
	return statusBody(w.s.b.CreateTopic(name))
}

func (w *wireConn) onPublish(payload []byte) []byte {
	r := sliceReader{b: payload}
	msg, err := readFields(&r)
	if err != nil || !finished(r) {
		return statusBody(ErrBadRequest)
	}
	published, err := w.s.b.Publish(msg)
	if err != nil {
		return statusBody(err)
	}
	var buf bytes.Buffer
	buf.Write(statusBody(nil))
	putU64(&buf, published.ID)
	putU64(&buf, published.Offset)
	putU64(&buf, uint64(published.Timestamp.UnixNano()))
	return buf.Bytes()
}

func (w *wireConn) onPublishBatch(payload []byte) []byte {
	r := sliceReader{b: payload}
	n, err := r.u32()
	if err != nil || n > maxBatch {
		return statusBody(ErrTooLarge)
	}
	if n == 0 {
		if !finished(r) {
			return statusBody(ErrBadRequest)
		}
		return statusBody(nil)
	}
	msgs := make([]Message, n)
	for i := uint32(0); i < n; i++ {
		msgs[i], err = readFields(&r)
		if err != nil {
			return statusBody(err)
		}
	}
	if !finished(r) {
		return statusBody(ErrBadRequest)
	}
	published, err := w.s.b.PublishBatch(msgs)
	if err != nil || len(published) != int(n) {
		if err == nil {
			err = ErrBadRequest
		}
		return statusBody(err)
	}
	var buf bytes.Buffer
	buf.Write(statusBody(nil))
	for _, m := range published {
		putU64(&buf, m.ID)
		putU64(&buf, m.Offset)
		putU64(&buf, uint64(m.Timestamp.UnixNano()))
	}
	return buf.Bytes()
}

func (w *wireConn) onSubscribe(payload []byte) []byte {
	r := sliceReader{b: payload}
	topic, err := r.str()
	if err != nil {
		return statusBody(ErrBadRequest)
	}
	group, err := r.str()
	if err != nil || !finished(r) {
		return statusBody(ErrBadRequest)
	}
	consumer, err := w.s.b.Subscribe(topic, group)
	if err != nil {
		return statusBody(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		cancel()
		return statusBody(ErrClosed)
	}
	if len(w.subs) >= w.s.b.cfg.MaxSubscriptions {
		w.mu.Unlock()
		cancel()
		w.s.b.hitLimit("subscriptions")
		return statusBody(ErrLimited)
	}
	w.nextSub++
	sub := &srvSub{id: w.nextSub, consumer: consumer, ctx: ctx, cancel: cancel}
	w.subs[sub.id] = sub
	w.mu.Unlock()
	var buf bytes.Buffer
	buf.Write(statusBody(nil))
	putU32(&buf, sub.id)
	return buf.Bytes()
}

func (w *wireConn) startReceive(corr uint32, payload []byte) {
	if err := w.permit(true); err != nil {
		w.writeFrame(corr, opReceive, statusBody(err))
		return
	}
	if len(payload) != 4 {
		w.writeFrame(corr, opReceive, statusBody(ErrBadRequest))
		return
	}
	id := binary.BigEndian.Uint32(payload)
	w.mu.Lock()
	sub := w.subs[id]
	closed := w.closed
	w.mu.Unlock()
	if closed {
		w.writeFrame(corr, opReceive, statusBody(ErrClosed))
		return
	}
	if sub == nil {
		w.writeFrame(corr, opReceive, statusBody(ErrBadRequest))
		return
	}
	w.wg.Add(1)
	go w.deliver(corr, sub)
}

func (w *wireConn) onSettle(payload []byte, ack bool) []byte {
	need := 12
	if !ack {
		need = 13
	}
	if len(payload) != need {
		return statusBody(ErrBadRequest)
	}
	subID := binary.BigEndian.Uint32(payload[0:4])
	token := binary.BigEndian.Uint64(payload[4:12])
	requeue := true
	if !ack && payload[12] > 1 {
		return statusBody(ErrBadRequest)
	}
	if !ack {
		requeue = payload[12] == 1
	}
	w.mu.Lock()
	tr, ok := w.inflight[token]
	if ok && tr.sub == subID {
		delete(w.inflight, token)
	} else {
		ok = false
	}
	w.mu.Unlock()
	if !ok {
		return statusBody(ErrSettled)
	}
	if ack {
		return statusBody(tr.d.Ack())
	}
	return statusBody(tr.d.Nack(requeue))
}

func (w *wireConn) onPoison(payload []byte) []byte {
	if len(payload) != 12 {
		return statusBody(ErrBadRequest)
	}
	subID := binary.BigEndian.Uint32(payload[0:4])
	token := binary.BigEndian.Uint64(payload[4:12])
	w.mu.Lock()
	tr, ok := w.inflight[token]
	if ok && tr.sub == subID {
		delete(w.inflight, token)
	} else {
		ok = false
	}
	w.mu.Unlock()
	if !ok {
		return statusBody(ErrSettled)
	}
	return statusBody(tr.d.Poison())
}

func (w *wireConn) deliver(corr uint32, sub *srvSub) {
	defer w.wg.Done()
	d, err := sub.consumer.Receive(sub.ctx)
	if err != nil {
		w.writeFrame(corr, opReceive, statusBody(err))
		return
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		d.Nack(true)
		return
	}
	w.nextTok++
	token := w.nextTok
	w.inflight[token] = tracked{d: d, sub: sub.id}
	w.mu.Unlock()
	body, err := encodeDeliver(sub.id, token, d.Message)
	if err != nil {
		w.mu.Lock()
		delete(w.inflight, token)
		w.mu.Unlock()
		d.Nack(true)
		return
	}
	var buf bytes.Buffer
	buf.Write(statusBody(nil))
	buf.Write(body)
	if w.writeFrame(corr, opReceive, buf.Bytes()) != nil {
		w.mu.Lock()
		delete(w.inflight, token)
		w.mu.Unlock()
		d.Nack(true)
	}
}

func encodeDeliver(sub uint32, token uint64, m Message) ([]byte, error) {
	var buf bytes.Buffer
	putU32(&buf, sub)
	putU64(&buf, token)
	putU64(&buf, m.ID)
	putU64(&buf, m.Offset)
	putU64(&buf, uint64(m.Timestamp.UnixNano()))
	if err := appendFields(&buf, m); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (w *wireConn) writeFrame(corr uint32, op uint16, payload []byte) error {
	buf, err := frameBytes(corr, op, payload)
	if err != nil {
		return err
	}
	w.wmu.Lock()
	defer w.wmu.Unlock()
	_, err = w.c.Write(buf)
	return err
}

func (w *wireConn) cleanup() {
	w.once.Do(func() {
		w.mu.Lock()
		w.closed = true
		cancels := make([]context.CancelFunc, 0, len(w.subs))
		for _, sub := range w.subs {
			cancels = append(cancels, sub.cancel)
		}
		w.mu.Unlock()
		for _, cancel := range cancels {
			cancel()
		}
		w.c.Close()
		w.s.b.connections.Add(-1)
		w.wg.Wait()
		w.mu.Lock()
		pending := w.inflight
		w.inflight = nil
		w.mu.Unlock()
		for _, tr := range pending {
			tr.d.Nack(true)
		}
	})
}
