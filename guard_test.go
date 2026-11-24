package forgemq

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"log/slog"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func TestTokenIsRequiredAndAPublisherCannotSubscribe(t *testing.T) {
	b := NewWith(Config{Principals: []Principal{
		{Token: "pub", Publish: true},
		{Token: "sub", Consume: true},
	}})
	s, err := Listen("127.0.0.1:0", b)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	t.Cleanup(func() { b.Close() })
	ctx := context.Background()
	if _, err := ConnectWith(s.Addr(), Options{Token: "nope"}); !errors.Is(err, ErrDenied) {
		t.Fatalf("bad token %v", err)
	}
	if b.Stats().AuthFailures == 0 {
		t.Fatal("auth failure was not counted")
	}
	plain, err := Connect(s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { plain.Close() })
	if err := plain.CreateTopic(ctx, "orders.created"); !errors.Is(err, ErrDenied) {
		t.Fatalf("anonymous %v", err)
	}
	pub, err := ConnectWith(s.Addr(), Options{Token: "pub"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pub.Close() })
	if err := pub.CreateTopic(ctx, "orders.created"); err != nil {
		t.Fatal(err)
	}
	if _, err := pub.Publish(ctx, Message{Topic: "orders.created", Payload: []byte("body")}); err != nil {
		t.Fatal(err)
	}
	if _, err := pub.Subscribe(ctx, "orders.created", "accounting"); !errors.Is(err, ErrDenied) {
		t.Fatalf("publisher subscribe %v", err)
	}
	sub, err := ConnectWith(s.Addr(), Options{Token: "sub"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sub.Close() })
	if _, err := sub.Publish(ctx, Message{Topic: "orders.created", Payload: []byte("no")}); !errors.Is(err, ErrDenied) {
		t.Fatalf("consumer publish %v", err)
	}
	subscription, err := sub.Subscribe(ctx, "orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	ev, err := subscription.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(ev.Payload) != "body" {
		t.Fatalf("payload %q", ev.Payload)
	}
	if err := ev.Poison(); err != nil {
		t.Fatal(err)
	}
	dlq, err := sub.Subscribe(ctx, "orders.created.dlq", "reader")
	if err != nil {
		t.Fatal(err)
	}
	dead, err := dlq.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if dead.Headers["forgemq-reason"] != "poison" {
		t.Fatalf("reason %q", dead.Headers["forgemq-reason"])
	}
}

func TestTLSCarriesAPublishAndAnAck(t *testing.T) {
	serverCfg, clientCfg := testTLS(t)
	b := NewWith(Config{Principals: []Principal{{Token: "t", Publish: true, Consume: true}}})
	s, err := ListenTLS("127.0.0.1:0", b, serverCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	t.Cleanup(func() { b.Close() })
	c, err := ConnectWith(s.Addr(), Options{Token: "t", TLS: clientCfg})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	ctx := context.Background()
	if err := c.CreateTopic(ctx, "orders.created"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Publish(ctx, Message{Topic: "orders.created", Payload: []byte("sealed")}); err != nil {
		t.Fatal(err)
	}
	sub, err := c.Subscribe(ctx, "orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	ev, err := sub.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(ev.Payload) != "sealed" {
		t.Fatalf("payload %q", ev.Payload)
	}
	if err := ev.Ack(); err != nil {
		t.Fatal(err)
	}
}

func TestConnectionTopicAndSubscriptionLimits(t *testing.T) {
	b := NewWith(Config{MaxConnections: 1, MaxTopics: 1, MaxSubscriptions: 1})
	s, err := Listen("127.0.0.1:0", b)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	t.Cleanup(func() { b.Close() })
	c1, err := Connect(s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c1.Close() })
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && b.Stats().Connections < 1 {
		time.Sleep(time.Millisecond)
	}
	if b.Stats().Connections != 1 {
		t.Fatalf("connections %d", b.Stats().Connections)
	}
	c2, err := Connect(s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c2.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c2.CreateTopic(ctx, "orders.created"); err == nil {
		t.Fatal("second connection was served")
	}
	if b.Stats().Limited == 0 {
		t.Fatal("limit was not counted")
	}
	if err := c1.CreateTopic(context.Background(), "orders.created"); err != nil {
		t.Fatal(err)
	}
	if err := c1.CreateTopic(context.Background(), "payments.completed"); !errors.Is(err, ErrLimited) {
		t.Fatalf("topics %v", err)
	}
	if _, err := c1.Publish(context.Background(), Message{Topic: "orders.created", Payload: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	sub, err := c1.Subscribe(context.Background(), "orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c1.Subscribe(context.Background(), "orders.created", "analytics"); !errors.Is(err, ErrLimited) {
		t.Fatalf("subscriptions %v", err)
	}
	ev, err := sub.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := ev.Ack(); err != nil {
		t.Fatal(err)
	}
}

func TestPoisonMessageStaysInOneGroup(t *testing.T) {
	var log lockedBuf
	b := NewWith(Config{Logger: slog.New(slog.NewTextHandler(&log, nil))})
	if err := b.CreateTopic("orders.created"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(Message{Topic: "orders.created", Payload: []byte("bad")}); err != nil {
		t.Fatal(err)
	}
	accounting, err := b.Subscribe("orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	analytics, err := b.Subscribe("orders.created", "analytics")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	poisoned, err := accounting.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := poisoned.Poison(); err != nil {
		t.Fatal(err)
	}
	kept, err := analytics.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(kept.Payload) != "bad" || kept.Offset != 0 {
		t.Fatalf("analytics %+v", kept.Message)
	}
	if err := kept.Ack(); err != nil {
		t.Fatal(err)
	}
	st := b.Stats()
	if st.Published != 1 || st.Delivered != 2 || st.Acked != 1 || st.DeadLettered != 1 {
		t.Fatalf("stats %+v", st)
	}
	if !strings.Contains(log.String(), "dead letter") || !strings.Contains(log.String(), "poison") {
		t.Fatalf("log %q", log.String())
	}
	dlq, err := b.Subscribe("orders.created.dlq", "reader")
	if err != nil {
		t.Fatal(err)
	}
	ev, err := dlq.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ev.Headers["forgemq-reason"] != "poison" || string(ev.Payload) != "bad" {
		t.Fatalf("dlq %+v", ev.Message)
	}
}

func TestInjectedFaultSkipsTheLog(t *testing.T) {
	b := New()
	if err := b.CreateTopic("orders.created"); err != nil {
		t.Fatal(err)
	}
	b.InjectPublishFaults(1)
	if _, err := b.Publish(Message{Topic: "orders.created", Payload: []byte("skip")}); !errors.Is(err, ErrFault) {
		t.Fatalf("fault %v", err)
	}
	msg, err := b.Publish(Message{Topic: "orders.created", Payload: []byte("kept")})
	if err != nil {
		t.Fatal(err)
	}
	if msg.Offset != 0 {
		t.Fatalf("offset %d", msg.Offset)
	}
	st := b.Stats()
	if st.Faults != 1 || st.Published != 1 {
		t.Fatalf("stats %+v", st)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	sub, err := b.Subscribe("orders.created", "accounting")
	if err != nil {
		t.Fatal(err)
	}
	ev, err := sub.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(ev.Payload) != "kept" {
		t.Fatalf("payload %q", ev.Payload)
	}
}

func testTLS(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	pool := x509.NewCertPool()
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool.AddCert(parsed)
	server := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	client := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, ServerName: "127.0.0.1"}
	return server, client
}
