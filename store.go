package forgemq

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	segmentHeaderLen = 32
	maxRecordBody    = 8 << 20
	nilBytes         = ^uint32(0)
)

type segFile struct {
	index int
	base  uint64
	count uint64
	size  int64
	path  string
	file  *os.File
}

type commitLog struct {
	mu      sync.Mutex
	file    *os.File
	size    int64
	commits map[string]uint64
}

func (b *Broker) load() error {
	if err := os.MkdirAll(filepath.Join(b.dir, "data"), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(b.dir, "metadata"), 0o755); err != nil {
		return err
	}
	if err := b.loadTopics(); err != nil {
		return err
	}
	log, err := openCommitLog(filepath.Join(b.dir, "metadata", "groups.log"))
	if err != nil {
		return err
	}
	b.commits = log
	return b.applyCommits(log.commits)
}

func (b *Broker) loadTopics() error {
	root := filepath.Join(b.dir, "data")
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if err := validName(name); err != nil {
			return ErrCorrupt
		}
		t, err := loadTopic(b, name)
		if err != nil {
			return err
		}
		b.topics[name] = t
		for i := range t.log {
			if t.log[i].ID > b.nextID {
				b.nextID = t.log[i].ID
			}
		}
	}
	return nil
}

func loadTopic(b *Broker, name string) (*topic, error) {
	t := newTopic(b, name)
	entries, err := os.ReadDir(t.dir)
	if err != nil {
		return nil, err
	}
	type numbered struct {
		index int
		path  string
	}
	var files []numbered
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		index, ok := segmentIndex(entry.Name())
		if !ok {
			continue
		}
		files = append(files, numbered{index, filepath.Join(t.dir, entry.Name())})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].index < files[j].index })
	if len(files) == 0 {
		if err := t.prepare(); err != nil {
			return nil, err
		}
		return t, nil
	}
	for i := 1; i < len(files); i++ {
		if files[i].index != files[i-1].index+1 {
			return nil, ErrCorrupt
		}
	}
	var prev *uint64
	for i, file := range files {
		seg, msgs, err := openSegment(file.path, name, i == len(files)-1, prev, file.index)
		if err != nil {
			return nil, err
		}
		if prev != nil && seg.base != *prev {
			if seg.file != nil {
				seg.file.Close()
			}
			return nil, ErrCorrupt
		}
		next := seg.base + seg.count
		prev = &next
		t.segs = append(t.segs, seg)
		t.log = append(t.log, msgs...)
	}
	t.base = t.segs[0].base
	t.active = t.segs[len(t.segs)-1]
	return t, nil
}

func openSegment(path, topic string, keep bool, prev *uint64, index int) (*segFile, []Message, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, nil, err
	}
	fail := func(err error) (*segFile, []Message, error) {
		f.Close()
		return nil, nil, err
	}
	st, err := f.Stat()
	if err != nil {
		return fail(err)
	}
	if st.Size() < segmentHeaderLen {
		if !keep || (prev == nil && index != 0) {
			return fail(ErrCorrupt)
		}
		base := uint64(0)
		if prev != nil {
			base = *prev
		}
		if err := rewriteHeader(f, base); err != nil {
			return fail(err)
		}
		seg := &segFile{index: index, base: base, size: segmentHeaderLen, path: path}
		if keep {
			seg.file = f
			return seg, nil, nil
		}
		f.Close()
		return seg, nil, nil
	}
	hdr := make([]byte, segmentHeaderLen)
	if _, err := f.ReadAt(hdr, 0); err != nil {
		return fail(err)
	}
	base, err := parseHeader(hdr)
	if err != nil {
		return fail(err)
	}
	if prev != nil && base != *prev {
		return fail(ErrCorrupt)
	}
	bodies, err := readFramed(f, segmentHeaderLen)
	if err != nil {
		return fail(err)
	}
	msgs := make([]Message, 0, len(bodies))
	expect := base
	for _, body := range bodies {
		msg, err := decodeMessage(body)
		if err != nil || msg.Topic != topic || msg.Offset != expect {
			return fail(ErrCorrupt)
		}
		msgs = append(msgs, msg)
		expect++
	}
	st, err = f.Stat()
	if err != nil {
		return fail(err)
	}
	seg := &segFile{
		index: index,
		base:  base,
		count: uint64(len(msgs)),
		size:  st.Size(),
		path:  path,
	}
	if keep {
		if _, err := f.Seek(st.Size(), io.SeekStart); err != nil {
			return fail(err)
		}
		seg.file = f
		return seg, msgs, nil
	}
	f.Close()
	return seg, msgs, nil
}

func (t *topic) prepare() error {
	if t.broker.dir == "" {
		return nil
	}
	if err := os.MkdirAll(t.dir, 0o755); err != nil {
		return err
	}
	if err := syncDir(filepath.Dir(t.dir)); err != nil {
		return err
	}
	if t.active != nil {
		return nil
	}
	seg, err := createSegment(t.dir, 0, 0)
	if err != nil {
		return err
	}
	t.segs = []*segFile{seg}
	t.active = seg
	t.base = 0
	return nil
}

func (t *topic) writeRecord(m Message) error {
	body, err := encodeMessage(m)
	if err != nil {
		return err
	}
	framed := frame(body)
	if t.active == nil {
		if err := t.prepare(); err != nil {
			return err
		}
	}
	if t.active.count > 0 && t.active.size+int64(len(framed)) > t.broker.segmentBytes {
		if err := t.roll(m.Offset); err != nil {
			return err
		}
	}
	n, err := t.active.file.Write(framed)
	if err != nil || n != len(framed) {
		t.rewindActive()
		if err == nil {
			err = io.ErrShortWrite
		}
		return err
	}
	if err := t.active.file.Sync(); err != nil {
		t.rewindActive()
		return err
	}
	t.active.count++
	t.active.size += int64(len(framed))
	return nil
}

func (t *topic) rewindActive() {
	_ = t.active.file.Truncate(t.active.size)
	_, _ = t.active.file.Seek(t.active.size, io.SeekStart)
}

func (t *topic) roll(base uint64) error {
	if err := t.active.file.Sync(); err != nil {
		return err
	}
	seg, err := createSegment(t.dir, t.active.index+1, base)
	if err != nil {
		return err
	}
	old := t.active
	t.segs = append(t.segs, seg)
	t.active = seg
	old.file.Close()
	old.file = nil
	return nil
}

func (t *topic) retain() {
	if t.broker.dir == "" {
		return
	}
	for len(t.segs) >= 2 {
		oldest := t.segs[0]
		if oldest.count == 0 || oldest.base != t.base || uint64(len(t.log)) < oldest.count {
			return
		}
		end := oldest.base + oldest.count
		if !t.covered(end) {
			return
		}
		if oldest.file != nil {
			oldest.file.Close()
			oldest.file = nil
		}
		if err := os.Remove(oldest.path); err != nil && !os.IsNotExist(err) {
			return
		}
		_ = syncDir(t.dir)
		t.segs = t.segs[1:]
		kept := make([]Message, uint64(len(t.log))-oldest.count)
		copy(kept, t.log[oldest.count:])
		t.log = kept
		t.base = end
	}
}

func (t *topic) covered(offset uint64) bool {
	if len(t.groups) == 0 {
		return false
	}
	for _, g := range t.groups {
		if g.commit < offset {
			return false
		}
	}
	return true
}

func (t *topic) closeFiles() {
	for _, seg := range t.segs {
		if seg.file != nil {
			seg.file.Close()
			seg.file = nil
		}
	}
}

func createSegment(dir string, index int, base uint64) (*segFile, error) {
	path := filepath.Join(dir, fmt.Sprintf("%06d.log", index))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	if err := rewriteHeader(f, base); err != nil {
		f.Close()
		os.Remove(path)
		return nil, err
	}
	if err := syncDir(dir); err != nil {
		f.Close()
		os.Remove(path)
		return nil, err
	}
	return &segFile{index: index, base: base, size: segmentHeaderLen, path: path, file: f}, nil
}

func rewriteHeader(f *os.File, base uint64) error {
	if err := f.Truncate(0); err != nil {
		return err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := f.Write(segmentHeader(base)); err != nil {
		return err
	}
	return f.Sync()
}

func segmentHeader(base uint64) []byte {
	buf := make([]byte, segmentHeaderLen)
	copy(buf[:4], "FGMQ")
	binary.BigEndian.PutUint16(buf[4:6], 1)
	binary.BigEndian.PutUint64(buf[8:16], base)
	return buf
}

func parseHeader(buf []byte) (uint64, error) {
	if len(buf) < segmentHeaderLen || string(buf[:4]) != "FGMQ" || binary.BigEndian.Uint16(buf[4:6]) != 1 {
		return 0, ErrCorrupt
	}
	return binary.BigEndian.Uint64(buf[8:16]), nil
}

func segmentIndex(name string) (int, bool) {
	if len(name) != len("000000.log") || !strings.HasSuffix(name, ".log") {
		return 0, false
	}
	for _, c := range name[:6] {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	n, err := strconv.Atoi(name[:6])
	if err != nil {
		return 0, false
	}
	return n, true
}

func openCommitLog(path string) (*commitLog, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	bodies, err := readFramed(f, 0)
	if err != nil {
		f.Close()
		return nil, err
	}
	commits := make(map[string]uint64, len(bodies))
	for _, body := range bodies {
		topic, group, commit, err := decodeCommit(body)
		if err != nil || validName(topic) != nil || validName(group) != nil {
			f.Close()
			return nil, ErrCorrupt
		}
		key := topic + "\x00" + group
		if commit > commits[key] {
			commits[key] = commit
		}
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(st.Size(), io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		f.Close()
		return nil, err
	}
	return &commitLog{file: f, size: st.Size(), commits: commits}, nil
}

func (b *Broker) applyCommits(commits map[string]uint64) error {
	for key, commit := range commits {
		topic, group, ok := strings.Cut(key, "\x00")
		if !ok {
			return ErrCorrupt
		}
		t := b.topics[topic]
		if t == nil {
			continue
		}
		if err := t.restore(group, commit); err != nil {
			return err
		}
	}
	return nil
}

func (t *topic) restore(name string, commit uint64) error {
	end := t.end()
	if commit < t.base || commit > end {
		return ErrCorrupt
	}
	cfg := t.broker.cfg
	t.groups[name] = &group{
		name:        name,
		commit:      commit,
		next:        commit,
		flight:      make(map[uint64]*flight),
		done:        make(map[uint64]bool),
		prefetch:    cfg.Prefetch,
		ackTimeout:  cfg.AckTimeout,
		maxAttempts: cfg.MaxAttempts,
	}
	return nil
}

func (b *Broker) appendCommit(topic, group string, commit uint64) error {
	if b.commits == nil {
		return nil
	}
	return b.commits.append(topic, group, commit)
}

func (c *commitLog) append(topic, group string, commit uint64) error {
	body, err := encodeCommit(topic, group, commit)
	if err != nil {
		return err
	}
	framed := frame(body)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.file == nil {
		return ErrClosed
	}
	n, err := c.file.Write(framed)
	if err != nil || n != len(framed) {
		c.rewind()
		if err == nil {
			err = io.ErrShortWrite
		}
		return err
	}
	if err := c.file.Sync(); err != nil {
		c.rewind()
		return err
	}
	c.size += int64(len(framed))
	key := topic + "\x00" + group
	if commit > c.commits[key] {
		c.commits[key] = commit
	}
	return nil
}

func (c *commitLog) rewind() {
	_ = c.file.Truncate(c.size)
	_, _ = c.file.Seek(c.size, io.SeekStart)
}

func (c *commitLog) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.file != nil {
		c.file.Close()
		c.file = nil
	}
}

func readFramed(f *os.File, start int64) ([][]byte, error) {
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := st.Size()
	pos := start
	var out [][]byte
	for pos < size {
		remain := size - pos
		if remain < 8 {
			if err := f.Truncate(pos); err != nil {
				return nil, err
			}
			return out, nil
		}
		var hdr [8]byte
		if _, err := f.ReadAt(hdr[:], pos); err != nil {
			return nil, err
		}
		bodyLen := binary.BigEndian.Uint32(hdr[0:4])
		sum := binary.BigEndian.Uint32(hdr[4:8])
		need := int64(bodyLen)
		if need > remain-8 {
			if err := f.Truncate(pos); err != nil {
				return nil, err
			}
			return out, nil
		}
		if bodyLen == 0 || need > maxRecordBody {
			if pos+8+need == size {
				if err := f.Truncate(pos); err != nil {
					return nil, err
				}
				return out, nil
			}
			return nil, ErrCorrupt
		}
		body := make([]byte, bodyLen)
		if _, err := f.ReadAt(body, pos+8); err != nil {
			return nil, err
		}
		if crc32.ChecksumIEEE(body) != sum {
			if pos+8+need == size {
				if err := f.Truncate(pos); err != nil {
					return nil, err
				}
				return out, nil
			}
			return nil, ErrCorrupt
		}
		out = append(out, body)
		pos += 8 + need
	}
	return out, nil
}

func frame(body []byte) []byte {
	buf := make([]byte, 8+len(body))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(body)))
	binary.BigEndian.PutUint32(buf[4:8], crc32.ChecksumIEEE(body))
	copy(buf[8:], body)
	return buf
}

func encodeMessage(m Message) ([]byte, error) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, m.ID); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, m.Offset); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, m.Timestamp.UTC().UnixNano()); err != nil {
		return nil, err
	}
	if err := putString(&buf, m.Topic); err != nil {
		return nil, err
	}
	if err := putBytes(&buf, m.Key); err != nil {
		return nil, err
	}
	if err := putBytes(&buf, m.Payload); err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(m.Headers))
	for k := range m.Headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if err := binary.Write(&buf, binary.BigEndian, uint32(len(keys))); err != nil {
		return nil, err
	}
	for _, k := range keys {
		if err := putString(&buf, k); err != nil {
			return nil, err
		}
		if err := putString(&buf, m.Headers[k]); err != nil {
			return nil, err
		}
	}
	if buf.Len() > maxRecordBody {
		return nil, ErrTooLarge
	}
	return buf.Bytes(), nil
}

func decodeMessage(body []byte) (Message, error) {
	r := sliceReader{b: body}
	var msg Message
	var err error
	if msg.ID, err = r.u64(); err != nil {
		return Message{}, err
	}
	if msg.Offset, err = r.u64(); err != nil {
		return Message{}, err
	}
	nano, err := r.i64()
	if err != nil {
		return Message{}, err
	}
	msg.Timestamp = time.Unix(0, nano).UTC()
	if msg.Topic, err = r.str(); err != nil {
		return Message{}, err
	}
	if msg.Key, err = r.bytes(); err != nil {
		return Message{}, err
	}
	if msg.Payload, err = r.bytes(); err != nil {
		return Message{}, err
	}
	n, err := r.u32()
	if err != nil {
		return Message{}, err
	}
	if n > 0 {
		msg.Headers = make(map[string]string, n)
		for i := uint32(0); i < n; i++ {
			k, err := r.str()
			if err != nil {
				return Message{}, err
			}
			v, err := r.str()
			if err != nil {
				return Message{}, err
			}
			msg.Headers[k] = v
		}
	}
	if r.i != len(r.b) {
		return Message{}, ErrCorrupt
	}
	return msg, nil
}

func encodeCommit(topic, group string, commit uint64) ([]byte, error) {
	var buf bytes.Buffer
	if err := putString(&buf, topic); err != nil {
		return nil, err
	}
	if err := putString(&buf, group); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, commit); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeCommit(body []byte) (string, string, uint64, error) {
	r := sliceReader{b: body}
	topic, err := r.str()
	if err != nil {
		return "", "", 0, err
	}
	group, err := r.str()
	if err != nil {
		return "", "", 0, err
	}
	commit, err := r.u64()
	if err != nil {
		return "", "", 0, err
	}
	if r.i != len(r.b) {
		return "", "", 0, ErrCorrupt
	}
	return topic, group, commit, nil
}

func putString(buf *bytes.Buffer, s string) error {
	if len(s) > maxRecordBody {
		return ErrTooLarge
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(len(s))); err != nil {
		return err
	}
	_, err := buf.WriteString(s)
	return err
}

func putBytes(buf *bytes.Buffer, b []byte) error {
	if b == nil {
		return binary.Write(buf, binary.BigEndian, nilBytes)
	}
	if len(b) >= int(nilBytes) || len(b) > maxRecordBody {
		return ErrTooLarge
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(len(b))); err != nil {
		return err
	}
	_, err := buf.Write(b)
	return err
}

type sliceReader struct {
	b []byte
	i int
}

func (r *sliceReader) take(n int) ([]byte, error) {
	if n < 0 || r.i+n > len(r.b) {
		return nil, ErrCorrupt
	}
	out := r.b[r.i : r.i+n]
	r.i += n
	return out, nil
}

func (r *sliceReader) u64() (uint64, error) {
	b, err := r.take(8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b), nil
}

func (r *sliceReader) u32() (uint32, error) {
	b, err := r.take(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}

func (r *sliceReader) i64() (int64, error) {
	n, err := r.u64()
	return int64(n), err
}

func (r *sliceReader) str() (string, error) {
	n, err := r.u32()
	if err != nil {
		return "", err
	}
	b, err := r.take(int(n))
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (r *sliceReader) bytes() ([]byte, error) {
	n, err := r.u32()
	if err != nil {
		return nil, err
	}
	if n == nilBytes {
		return nil, nil
	}
	b, err := r.take(int(n))
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out, nil
}

func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	err = f.Sync()
	cerr := f.Close()
	if err != nil {
		if syncUnsupported(err) {
			return nil
		}
		return err
	}
	return cerr
}

func syncUnsupported(err error) bool {
	var errno syscall.Errno
	if !errors.As(err, &errno) {
		return false
	}
	switch errno {
	case syscall.EINVAL, syscall.ENOTSUP, syscall.EPERM, 1, 5:
		return true
	default:
		return false
	}
}
