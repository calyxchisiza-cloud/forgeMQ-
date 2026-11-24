package forgemq

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"sort"
)

const (
	maxFrame    = maxRecordBody + 64<<10
	framePrefix = 6
	maxHeaders  = 1024
	maxBatch    = 256

	opCreateTopic  uint16 = 1
	opPublish      uint16 = 2
	opSubscribe    uint16 = 3
	opAck          uint16 = 4
	opNack         uint16 = 5
	opDeliver      uint16 = 6
	opReceive      uint16 = 7
	opAuth         uint16 = 8
	opPoison       uint16 = 9
	opPublishBatch uint16 = 10

	statusOK uint16 = iota
	statusExists
	statusNotFound
	statusInvalid
	statusClosed
	statusSettled
	statusTooLarge
	statusBad
	statusDenied
	statusLimited
	statusFault
)

func frameBytes(corr uint32, op uint16, payload []byte) ([]byte, error) {
	if len(payload) > maxFrame-framePrefix {
		return nil, ErrTooLarge
	}
	body := make([]byte, framePrefix+len(payload))
	binary.BigEndian.PutUint32(body[0:4], corr)
	binary.BigEndian.PutUint16(body[4:6], op)
	copy(body[6:], payload)
	out := make([]byte, 8+len(body))
	binary.BigEndian.PutUint32(out[0:4], uint32(len(body)))
	binary.BigEndian.PutUint32(out[4:8], crc32.ChecksumIEEE(body))
	copy(out[8:], body)
	return out, nil
}

func readFrame(r io.Reader) (uint32, uint16, []byte, error) {
	var hdr [8]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, 0, nil, err
	}
	n := binary.BigEndian.Uint32(hdr[0:4])
	sum := binary.BigEndian.Uint32(hdr[4:8])
	if n < framePrefix || n > maxFrame {
		return 0, 0, nil, ErrCorrupt
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, 0, nil, err
	}
	if crc32.ChecksumIEEE(body) != sum {
		return 0, 0, nil, ErrCorrupt
	}
	corr := binary.BigEndian.Uint32(body[0:4])
	op := binary.BigEndian.Uint16(body[4:6])
	return corr, op, body[6:], nil
}

func statusCode(err error) uint16 {
	switch {
	case err == nil:
		return statusOK
	case errors.Is(err, ErrTopicExists):
		return statusExists
	case errors.Is(err, ErrTopicNotFound):
		return statusNotFound
	case errors.Is(err, ErrInvalidName):
		return statusInvalid
	case errors.Is(err, ErrClosed):
		return statusClosed
	case errors.Is(err, ErrSettled):
		return statusSettled
	case errors.Is(err, ErrTooLarge):
		return statusTooLarge
	case errors.Is(err, ErrDenied):
		return statusDenied
	case errors.Is(err, ErrLimited):
		return statusLimited
	case errors.Is(err, ErrFault):
		return statusFault
	default:
		return statusBad
	}
}

func statusError(code uint16) error {
	switch code {
	case statusOK:
		return nil
	case statusExists:
		return ErrTopicExists
	case statusNotFound:
		return ErrTopicNotFound
	case statusInvalid:
		return ErrInvalidName
	case statusClosed:
		return ErrClosed
	case statusSettled:
		return ErrSettled
	case statusTooLarge:
		return ErrTooLarge
	case statusDenied:
		return ErrDenied
	case statusLimited:
		return ErrLimited
	case statusFault:
		return ErrFault
	default:
		return ErrBadRequest
	}
}

func statusBody(err error) []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, statusCode(err))
	return buf
}

func splitStatus(p []byte) (uint16, []byte, error) {
	if len(p) < 2 {
		return 0, nil, ErrBadRequest
	}
	return binary.BigEndian.Uint16(p[:2]), p[2:], nil
}

func appendFields(buf *bytes.Buffer, m Message) error {
	if err := putString(buf, m.Topic); err != nil {
		return err
	}
	if err := putBytes(buf, m.Key); err != nil {
		return err
	}
	if err := putBytes(buf, m.Payload); err != nil {
		return err
	}
	keys := make([]string, 0, len(m.Headers))
	for k := range m.Headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if len(keys) > maxHeaders {
		return ErrTooLarge
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(len(keys))); err != nil {
		return err
	}
	for _, k := range keys {
		if err := putString(buf, k); err != nil {
			return err
		}
		if err := putString(buf, m.Headers[k]); err != nil {
			return err
		}
	}
	return nil
}

func readFields(r *sliceReader) (Message, error) {
	var m Message
	var err error
	if m.Topic, err = r.str(); err != nil {
		return Message{}, err
	}
	if m.Key, err = r.bytes(); err != nil {
		return Message{}, err
	}
	if m.Payload, err = r.bytes(); err != nil {
		return Message{}, err
	}
	n, err := r.u32()
	if err != nil {
		return Message{}, err
	}
	if n > maxHeaders {
		return Message{}, ErrBadRequest
	}
	if n > 0 {
		m.Headers = make(map[string]string, n)
		for i := uint32(0); i < n; i++ {
			k, err := r.str()
			if err != nil {
				return Message{}, err
			}
			v, err := r.str()
			if err != nil {
				return Message{}, err
			}
			m.Headers[k] = v
		}
	}
	return m, nil
}

func finished(r sliceReader) bool {
	return r.i == len(r.b)
}

func putU32(buf *bytes.Buffer, n uint32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], n)
	buf.Write(b[:])
}

func putU64(buf *bytes.Buffer, n uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], n)
	buf.Write(b[:])
}
