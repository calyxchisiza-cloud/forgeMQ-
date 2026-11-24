package forgemq

import "sort"

// TopicInfo describes one topic without joining any group.
type TopicInfo struct {
	Name    string      `json:"name"`
	Base    uint64      `json:"base"`
	End     uint64      `json:"end"`
	Records int         `json:"records"`
	Groups  []GroupInfo `json:"groups"`
}

// GroupInfo is a group's finished offset. Behind is how many retained
// records sit at or after that offset. Reading this does not move it.
type GroupInfo struct {
	Name   string `json:"name"`
	Commit uint64 `json:"commit"`
	Behind uint64 `json:"behind"`
}

// Page is a slice of a topic log. Messages stay in offset order.
type Page struct {
	Name     string
	Base     uint64
	End      uint64
	From     uint64
	Groups   []GroupInfo
	Messages []Message
}

// Topics lists every topic, including dead-letter logs.
func (b *Broker) Topics() ([]TopicInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrClosed
	}
	out := make([]TopicInfo, 0, len(b.topics))
	for _, t := range b.topics {
		out = append(out, t.snapshot())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Inspect copies records from a topic. from is the first offset to show.
// When tail is set, the page ends at the newest retained record.
// The call does not subscribe and does not move a group offset.
func (b *Broker) Inspect(topic string, from uint64, limit int, tail bool) (Page, error) {
	if err := validName(topic); err != nil {
		return Page{}, err
	}
	if limit <= 0 {
		limit = 40
	}
	if limit > 100 {
		limit = 100
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return Page{}, ErrClosed
	}
	t := b.topics[topic]
	b.mu.Unlock()
	if t == nil {
		return Page{}, ErrTopicNotFound
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return Page{}, ErrClosed
	}
	end := t.end()
	start := from
	if start < t.base {
		start = t.base
	}
	if start > end {
		start = end
	}
	if tail {
		start = t.base
		if end > t.base && end-t.base > uint64(limit) {
			start = end - uint64(limit)
		}
	}
	n := 0
	if start < end {
		n = int(end - start)
		if n > limit {
			n = limit
		}
	}
	msgs := make([]Message, n)
	for i := 0; i < n; i++ {
		msgs[i] = cloneMessage(t.at(start + uint64(i)))
	}
	return Page{
		Name:     t.name,
		Base:     t.base,
		End:      end,
		From:     start,
		Groups:   t.groupInfos(end),
		Messages: msgs,
	}, nil
}

func (t *topic) snapshot() TopicInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	end := t.end()
	return TopicInfo{
		Name:    t.name,
		Base:    t.base,
		End:     end,
		Records: int(end - t.base),
		Groups:  t.groupInfos(end),
	}
}

func (t *topic) groupInfos(end uint64) []GroupInfo {
	out := make([]GroupInfo, 0, len(t.groups))
	for _, g := range t.groups {
		behind := uint64(0)
		if g.commit < end {
			behind = end - g.commit
		}
		out = append(out, GroupInfo{Name: g.name, Commit: g.commit, Behind: behind})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
