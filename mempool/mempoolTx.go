package mempool

import (
	"sync"
	"sync/atomic"

	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/types"
)

// mempoolTx is an entry in the mempool.
type mempoolTx struct {
	height    int64    // height that this tx had been validated in
	gasWanted int64    // amount of gas this tx states it will require
	tx        types.Tx // validated by the application

	// ids of peers who've sent us this tx (as a map for quick lookups).
	// senders: PeerID -> struct{}
	senders sync.Map
	nonce   []byte
}

// Height returns the height for this transaction.
func (memTx *mempoolTx) Height() int64 {
	return atomic.LoadInt64(&memTx.height)
}

// Add the peer ID to the list of senders. Return true iff it exists already in the list.
func (memTx *mempoolTx) addSender(peerID p2p.ID) bool {
	if len(peerID) == 0 {
		return false
	}
	if _, loaded := memTx.senders.LoadOrStore(peerID, struct{}{}); loaded {
		return true
	}
	return false
}

func (memTx *mempoolTx) Senders() []p2p.ID {
	senders := make([]p2p.ID, 0)
	memTx.senders.Range(func(key, _ any) bool {
		senders = append(senders, key.(p2p.ID))
		return true
	})
	return senders
}

func (memTx *mempoolTx) PickOneSender() p2p.ID {
	sender := noSender
	memTx.senders.Range(func(key, _ any) bool {
		sender = key.(p2p.ID)
		return false
	})
	return sender
}
