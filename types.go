package capacitor

import (
	"github.com/vmihailenco/msgpack/v5"
)

//go:generate msgp

// MsgType defines the type of gossip message.
type MsgType byte

const (
	MsgSet MsgType = iota
	MsgIncr
	MsgMetric
	MsgWindow
)

// Message is the container for all gossip data.
type Message struct {
	Type      MsgType `msg:"t"`
	Key       string  `msg:"k"`
	Value     []byte  `msg:"v,omitempty"`
	ValueObj  any     `msg:"-"` // Used for lazy serialization
	Delta     float64 `msg:"d,omitempty"`
	TTL       int64   `msg:"ttl,omitempty"` // Seconds
	Timestamp int64   `msg:"ts"`            // UnixNano for LWW
	NodeID    string  `msg:"n"`             // Source node
}

// Metric represents a composite historical value tracking frequency and volume.
type Metric struct {
	Count int64   `json:"count" msg:"c"`
	Sum   float64 `json:"sum" msg:"s"`
}

// Encode serializes the message to MsgPack.
func (m *Message) Encode() ([]byte, error) {
	if m.ValueObj != nil && m.Value == nil {
		m.Value, _ = msgpack.Marshal(m.ValueObj)
	}
	return msgpack.Marshal(m)
}

// Decode deserializes the message from MsgPack.
func Decode(data []byte) (*Message, error) {
	var m Message
	if err := msgpack.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}
