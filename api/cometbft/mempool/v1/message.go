package v1

import (
	"fmt"

	"github.com/cosmos/gogoproto/proto"
)

// Wrap implements the p2p Wrapper interface and wraps a mempool message.
func (m *Tx) Wrap() proto.Message {
	mm := &Message{}
	mm.Sum = &Message_Tx{Tx: m}
	return mm
}

// TODO: Rename Message_TxKey to Message_HaveTx  (in .proto)
func (m *HaveTx) Wrap() proto.Message {
	mm := &Message{}
	mm.Sum = &Message_HaveTx{HaveTx: m}
	return mm
}

func (m *Reset) Wrap() proto.Message {
	mm := &Message{}
	mm.Sum = &Message_Reset_{Reset_: m}
	return mm
}

// Unwrap implements the p2p Wrapper interface and unwraps a wrapped mempool
// message.
func (m *Message) Unwrap() (proto.Message, error) {
	switch msg := m.Sum.(type) {
	case *Message_Tx:
		return m.GetTx(), nil
	case *Message_HaveTx:
		return m.GetHaveTx(), nil
	case *Message_Reset_:
		return m.GetReset_(), nil

	default:
		return nil, fmt.Errorf("unknown message: %T", msg)
	}
}
