package broker

import (
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"time"
)

const (
	brokerFrameNotifyMagic        = "NMFN"
	brokerFrameNotifyVersion      = 1
	brokerFrameNotifyPacketSize   = 24
	brokerFrameNotifyDialRetry    = 1 * time.Second
	brokerFrameNotifyWriteTimeout = 2 * time.Millisecond
)

type brokerFrameNotification struct {
	StreamID    byte
	Flags       uint16
	FrameID     uint64
	TimestampNS uint64
}

type brokerFrameNotifyResult struct {
	Sent      bool
	Attempted bool
	Err       error
}

type brokerSubFrameNotifier struct {
	mu           sync.Mutex
	path         string
	conn         *net.UnixConn
	nextDialAt   time.Time
	dialRetry    time.Duration
	writeTimeout time.Duration
}

func marshalBrokerFrameNotification(notification brokerFrameNotification) []byte {
	packet := make([]byte, brokerFrameNotifyPacketSize)
	copy(packet[:4], []byte(brokerFrameNotifyMagic))
	packet[4] = brokerFrameNotifyVersion
	packet[5] = notification.StreamID
	binary.BigEndian.PutUint16(packet[6:8], notification.Flags)
	binary.BigEndian.PutUint64(packet[8:16], notification.FrameID)
	binary.BigEndian.PutUint64(packet[16:24], notification.TimestampNS)
	return packet
}

func parseBrokerFrameNotification(packet []byte) (brokerFrameNotification, error) {
	if len(packet) != brokerFrameNotifyPacketSize {
		return brokerFrameNotification{}, errors.New("invalid broker frame notification size")
	}
	if string(packet[:4]) != brokerFrameNotifyMagic {
		return brokerFrameNotification{}, errors.New("invalid broker frame notification magic")
	}
	if packet[4] != brokerFrameNotifyVersion {
		return brokerFrameNotification{}, errors.New("unsupported broker frame notification version")
	}
	return brokerFrameNotification{
		StreamID:    packet[5],
		Flags:       binary.BigEndian.Uint16(packet[6:8]),
		FrameID:     binary.BigEndian.Uint64(packet[8:16]),
		TimestampNS: binary.BigEndian.Uint64(packet[16:24]),
	}, nil
}

func newBrokerSubFrameNotifier(path string) *brokerSubFrameNotifier {
	if path == "" {
		return nil
	}
	return &brokerSubFrameNotifier{
		path:         path,
		dialRetry:    brokerFrameNotifyDialRetry,
		writeTimeout: brokerFrameNotifyWriteTimeout,
	}
}

func (n *brokerSubFrameNotifier) Notify(frameID uint32, timestampNS uint64, flags uint16, now time.Time) brokerFrameNotifyResult {
	if n == nil || n.path == "" {
		return brokerFrameNotifyResult{}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.conn == nil {
		if !n.nextDialAt.IsZero() && now.Before(n.nextDialAt) {
			return brokerFrameNotifyResult{}
		}
		addr := net.UnixAddr{Name: n.path, Net: "unixgram"}
		conn, err := net.DialUnix("unixgram", nil, &addr)
		if err != nil {
			n.nextDialAt = now.Add(n.dialRetry)
			return brokerFrameNotifyResult{Attempted: true, Err: err}
		}
		n.conn = conn
	}

	packet := marshalBrokerFrameNotification(brokerFrameNotification{
		StreamID:    brokerStreamNV12,
		Flags:       flags,
		FrameID:     uint64(frameID),
		TimestampNS: timestampNS,
	})
	if n.writeTimeout > 0 {
		_ = n.conn.SetWriteDeadline(now.Add(n.writeTimeout))
	}
	_, err := n.conn.Write(packet)
	if n.writeTimeout > 0 {
		_ = n.conn.SetWriteDeadline(time.Time{})
	}
	if err != nil {
		_ = n.conn.Close()
		n.conn = nil
		n.nextDialAt = now.Add(n.dialRetry)
		return brokerFrameNotifyResult{Attempted: true, Err: err}
	}
	return brokerFrameNotifyResult{Sent: true, Attempted: true}
}

func (n *brokerSubFrameNotifier) Close() error {
	if n == nil {
		return nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.conn == nil {
		return nil
	}
	err := n.conn.Close()
	n.conn = nil
	return err
}

func (n *brokerSubFrameNotifier) Rebind(path string) {
	if n == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.conn != nil {
		_ = n.conn.Close()
		n.conn = nil
	}
	n.path = path
	n.nextDialAt = time.Time{}
}
