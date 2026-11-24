package forgemq

import "crypto/subtle"

// Stats counts work since the broker was opened.
type Stats struct {
	Published    uint64
	Delivered    uint64
	Acked        uint64
	Nacked       uint64
	DeadLettered uint64
	AuthFailures uint64
	Limited      uint64
	Connections  uint64
	Faults       uint64
}

func (b *Broker) Stats() Stats {
	conns := b.connections.Load()
	if conns < 0 {
		conns = 0
	}
	return Stats{
		Published:    b.published.Load(),
		Delivered:    b.delivered.Load(),
		Acked:        b.acked.Load(),
		Nacked:       b.nacked.Load(),
		DeadLettered: b.dead.Load(),
		AuthFailures: b.authFailures.Load(),
		Limited:      b.limited.Load(),
		Connections:  uint64(conns),
		Faults:       b.faultCount.Load(),
	}
}

// InjectPublishFaults makes the next n Publish calls fail before a record is stored.
func (b *Broker) InjectPublishFaults(n int) {
	if n < 0 {
		n = 0
	}
	b.faults.Store(int64(n))
}

func (b *Broker) takeFault() bool {
	for {
		n := b.faults.Load()
		if n <= 0 {
			return false
		}
		if b.faults.CompareAndSwap(n, n-1) {
			b.faultCount.Add(1)
			b.logInfo("publish fault injected")
			return true
		}
	}
}

func (b *Broker) secured() bool {
	return len(b.principals) > 0
}

func (b *Broker) principal(token string) (bool, bool, error) {
	if !b.secured() {
		return true, true, nil
	}
	tb := []byte(token)
	found := false
	var publish, consume bool
	for _, p := range b.principals {
		if subtle.ConstantTimeCompare(tb, []byte(p.Token)) == 1 && !found {
			publish = p.Publish
			consume = p.Consume
			found = true
		}
	}
	if !found {
		return false, false, ErrDenied
	}
	return publish, consume, nil
}

func (b *Broker) deny() {
	b.authFailures.Add(1)
	b.logInfo("not allowed")
}

func (b *Broker) hitLimit(kind string) {
	b.limited.Add(1)
	b.logInfo("limit reached", "kind", kind)
}

func (b *Broker) logInfo(msg string, args ...any) {
	if b == nil || b.logger == nil {
		return
	}
	b.logger.Info(msg, args...)
}
