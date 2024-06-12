package mempool

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"

	abcicli "github.com/cometbft/cometbft/abci/client"
	protomem "github.com/cometbft/cometbft/api/cometbft/mempool/v1"
	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/internal/clist"
	"github.com/cometbft/cometbft/libs/log"
	cmtsync "github.com/cometbft/cometbft/libs/sync"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/types"
)

const (
	noSender = p2p.ID("")
	nonceLen = 8
)

// Reactor handles mempool tx broadcasting amongst peers.
// It maintains a map from peer ID to counter, to prevent gossiping txs to the
// peers you received it from.
type Reactor struct {
	p2p.BaseReactor
	config  *cfg.MempoolConfig
	mempool *CListMempool

	waitSync   atomic.Bool
	waitSyncCh chan struct{} // for signaling when to start receiving and sending txs

	// Enabling/disabling routes for disseminating txs.
	router *gossipRouter

	// Semaphores to keep track of how many connections to peers are active for broadcasting
	// transactions. Each semaphore has a capacity that puts an upper bound on the number of
	// connections for different groups of peers.
	activePersistentPeersSemaphore    *semaphore.Weighted
	activeNonPersistentPeersSemaphore *semaphore.Weighted
}

// NewReactor returns a new Reactor with the given config and mempool.
func NewReactor(config *cfg.MempoolConfig, mempool *CListMempool, waitSync bool) *Reactor {
	memR := &Reactor{
		config:   config,
		mempool:  mempool,
		waitSync: atomic.Bool{},
	}
	memR.BaseReactor = *p2p.NewBaseReactor("Mempool", memR)
	if waitSync {
		memR.waitSync.Store(true)
		memR.waitSyncCh = make(chan struct{})
	}
	memR.activePersistentPeersSemaphore = semaphore.NewWeighted(int64(memR.config.ExperimentalMaxGossipConnectionsToPersistentPeers))
	memR.activeNonPersistentPeersSemaphore = semaphore.NewWeighted(int64(memR.config.ExperimentalMaxGossipConnectionsToNonPersistentPeers))

	return memR
}

// SetLogger sets the Logger on the reactor and the underlying mempool.
func (memR *Reactor) SetLogger(l log.Logger) {
	memR.Logger = l
	memR.mempool.SetLogger(l)
}

// OnStart implements p2p.BaseReactor.
func (memR *Reactor) OnStart() error {
	if memR.WaitSync() {
		memR.Logger.Info("Starting reactor in sync mode: tx propagation will start once sync completes")
	}
	if !memR.config.Broadcast {
		memR.Logger.Info("Tx broadcasting is disabled")
	}
	memR.router = newGossipRouter()
	return nil
}

// GetChannels implements Reactor by returning the list of channels for this
// reactor.
func (memR *Reactor) GetChannels() []*p2p.ChannelDescriptor {
	largestTx := make([]byte, memR.config.MaxTxBytes)
	batchMsg := protomem.Message{Sum: &protomem.Message_Tx{Tx: &protomem.Tx{Tx: largestTx}}}

	key := types.Tx(largestTx).Key()
	haveTxMsg := protomem.Message{Sum: &protomem.Message_HaveTx{HaveTx: &protomem.HaveTx{TxKey: key[:]}}}

	return []*p2p.ChannelDescriptor{
		{
			ID:                  MempoolControlChannel,
			Priority:            20,
			RecvMessageCapacity: haveTxMsg.Size(),
			MessageType:         &protomem.Message{},
		},
		{
			ID:                  MempoolChannel,
			Priority:            5,
			RecvMessageCapacity: batchMsg.Size(),
			MessageType:         &protomem.Message{},
		},
	}
}

// AddPeer implements Reactor.
// It starts a broadcast routine ensuring all txs are forwarded to the given peer.
func (memR *Reactor) AddPeer(peer p2p.Peer) {
	if memR.config.Broadcast {
		go func() {
			// Always forward transactions to unconditional peers.
			if !memR.Switch.IsPeerUnconditional(peer.ID()) {
				// Depending on the type of peer, we choose a semaphore to limit the gossiping peers.
				var peerSemaphore *semaphore.Weighted
				if peer.IsPersistent() && memR.config.ExperimentalMaxGossipConnectionsToPersistentPeers > 0 {
					peerSemaphore = memR.activePersistentPeersSemaphore
				} else if !peer.IsPersistent() && memR.config.ExperimentalMaxGossipConnectionsToNonPersistentPeers > 0 {
					peerSemaphore = memR.activeNonPersistentPeersSemaphore
				}

				if peerSemaphore != nil {
					for peer.IsRunning() {
						// Block on the semaphore until a slot is available to start gossiping with this peer.
						// Do not block indefinitely, in case the peer is disconnected before gossiping starts.
						ctxTimeout, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
						// Block sending transactions to peer until one of the connections become
						// available in the semaphore.
						err := peerSemaphore.Acquire(ctxTimeout, 1)
						cancel()

						if err != nil {
							continue
						}

						// Release semaphore to allow other peer to start sending transactions.
						defer peerSemaphore.Release(1)
						break
					}
				}
			}

			memR.mempool.metrics.ActiveOutboundConnections.Add(1)
			defer memR.mempool.metrics.ActiveOutboundConnections.Add(-1)
			memR.broadcastTxRoutine(peer)
		}()
	}
}

func (memR *Reactor) RemovePeer(peer p2p.Peer, _ any) {
	memR.Logger.Info("🟡 Remove peer: send Reset to other peers", "peer", peer.ID())

	// Remove all routes with peer as source or target.
	memR.router.resetRoutes(peer.ID())

	/*
		// Broadcast Reset to all peers except sender.
		memR.Switch.Peers().ForEach(func(p p2p.Peer) {
			if p.ID() != peer.ID() {
				memR.SendReset(p)
			}
		})
	*/
	memR.mempool.metrics.NumDisabledRoutes.Set(float64(memR.router.numRoutes()))
}

// Receive implements Reactor.
// It adds any received transactions to the mempool.
func (memR *Reactor) Receive(e p2p.Envelope) {
	if memR.WaitSync() {
		memR.Logger.Debug("Ignore message received while syncing", "src", e.Src, "chId", e.ChannelID, "msg", e.Message)
		return
	}

	memR.Logger.Debug("Receive", "src", e.Src, "chId", e.ChannelID, "msg", e.Message)
	senderID := e.Src.ID()

	switch e.ChannelID {
	case MempoolControlChannel:
		switch msg := e.Message.(type) {
		case *protomem.HaveTx:
			txKey := types.TxKey(msg.GetTxKey())
			memR.Logger.Info("🟠 Received HaveTx message", "from", senderID.ShortString(), "tx", txKey.String())

			sources, err := memR.mempool.GetSenders(txKey)
			if err != nil || len(sources) == 0 || sources[0] == noSender {
				// Probably tx and sender got removed from the mempool.
				memR.Logger.Info("👻 Received HaveTx but failed to get sender", "tx", txKey.String(), "err", err)
				return
			}

			// do not gossip to peer that send us HaveTx any tx coming from the source of txKey
			// TODO: Pick a random one
			source := sources[0]
			memR.router.disableRoute(source, senderID)
			memR.Logger.Info("⛔️ Disable route", "source", source.ShortString(), "target", senderID.ShortString())
			memR.mempool.metrics.TotalHaveTxMsgsReceived.With("from", string(senderID)).Add(1)
			memR.mempool.metrics.NumDisabledRoutes.Set(float64(memR.router.numRoutes()))

		case *protomem.Reset:
			memR.router.resetRoutes(e.Src.ID())
			memR.Logger.Info("🧹 Received Reset message", "from", e.Src.ID().ShortString())
			memR.mempool.metrics.NumDisabledRoutes.Set(float64(memR.router.numRoutes()))

		default:
			memR.Logger.Error("unknown message type", "src", e.Src, "chId", e.ChannelID, "msg", e.Message)
			memR.Switch.StopPeerForError(e.Src, fmt.Errorf("mempool cannot handle message of type: %T", e.Message))
		}

	case MempoolChannel:
		switch msg := e.Message.(type) {
		case *protomem.Tx:
			txBytes := msg.GetTx()
			if len(txBytes) == 0 {
				memR.Logger.Error("received empty tx from peer", "src", e.Src)
				return
			}
			tx := types.Tx(txBytes)
			_, _ = memR.tryAddTxWithSender(tx, e.Src, msg.GetNonce())
		default:
			memR.Logger.Error("unknown message type", "src", e.Src, "chId", e.ChannelID, "msg", e.Message)
			memR.Switch.StopPeerForError(e.Src, fmt.Errorf("mempool cannot handle message of type: %T", e.Message))
		}

	default:
		memR.Logger.Error("unknown message channel", "src", e.Src, "chId", e.ChannelID, "msg", e.Message)
		memR.Switch.StopPeerForError(e.Src, fmt.Errorf("mempool cannot handle message on channel: %T", e.Message))
	}

	// broadcasting happens from go routines per peer
}

// For txs added from RPC endpoints, that is, without sender or nonce.
func (memR *Reactor) TryAddTx(tx types.Tx) (*abcicli.ReqRes, error) {
	return memR.tryAddTxWithSender(tx, nil, nil)
}

// When source is nil, it means that the transaction comes from an RPC endpoint.
func (memR *Reactor) tryAddTxWithSender(tx types.Tx, sender p2p.Peer, nonce []byte) (*abcicli.ReqRes, error) {
	txKey := tx.Key()
	txKeyString := txKey.String()
	senderID := noSender
	if sender != nil {
		senderID = sender.ID()
	}
	memR.Logger.Debug("🔔 tryAddTxWithSender", "sender", senderID.ShortString(), "tx", txKeyString, "nonce", nonce)

	reqRes, err := memR.mempool.CheckTx(tx, senderID, nonce)
	switch {
	case errors.Is(err, ErrTxInCache):
		memR.Logger.Error("👯 Tx already exists in cache with different nonce", "sender", senderID.ShortString(), "tx", txKeyString)
		// do not reply HaveTx: same tx have different origins. Possible attack

	case errors.Is(err, ErrTxInCacheSameNonce):
		memR.Logger.Debug("👯 Tx already exists in cache with the same nonce", "sender", senderID.ShortString(), "tx", txKeyString)
		if sender != nil {
			memR.router.incDuplicateTxs()
			if !memR.router.isHaveTxBlocked() {
				if !sender.Send(p2p.Envelope{ChannelID: MempoolControlChannel, Message: &protomem.HaveTx{TxKey: txKey[:]}}) {
					memR.Logger.Error("Failed to send HaveTx", "tx", txKeyString)
				} else {
					memR.Logger.Info("🔵 Send HaveTx message", "to", senderID.ShortString(), "tx", txKeyString)
					memR.router.setBlockHaveTx()
				}
			} else {
				memR.Logger.Debug("🟤 Didn't send HaveTx message, sending is blocked")
			}
		} else {
			memR.Logger.Error("OH my god! This shouldn't happen!")
		}
		return nil, err

	case err != nil:
		memR.Logger.Info("Could not check tx", "tx", txKeyString, "err", err)
		return nil, err
	}

	// adjust redundancy
	memR.router.incFirstTimeTx()
	redundancy, sendReset := memR.router.adjustRedundancy(memR.Logger)
	if sendReset {
		// Send Reset to a random peer.
		p := memR.Switch.Peers().Random()
		memR.SendReset(p)
	}

	// update metrics
	if redundancy >= 0 {
		memR.mempool.metrics.Redundancy.Set(redundancy)
	}

	return reqRes, nil
}

func (memR *Reactor) SendReset(p p2p.Peer) {
	if !p.Send(p2p.Envelope{ChannelID: MempoolControlChannel, Message: &protomem.Reset{}}) {
		memR.Logger.Error("Failed to send Reset", "peer", p.ID())
	}
	memR.mempool.metrics.TotalResetMsgsSent.Add(1)
}

func (memR *Reactor) EnableInOutTxs() {
	memR.Logger.Info("enabling inbound and outbound transactions")
	if !memR.waitSync.CompareAndSwap(true, false) {
		return
	}

	// Releases all the blocked broadcastTxRoutine instances.
	if memR.config.Broadcast {
		close(memR.waitSyncCh)
	}
}

func (memR *Reactor) WaitSync() bool {
	return memR.waitSync.Load()
}

// PeerState describes the state of a peer.
type PeerState interface {
	GetHeight() int64
}

// Send new mempool txs to peer.
func (memR *Reactor) broadcastTxRoutine(peer p2p.Peer) {
	var next *clist.CElement

	// If the node is catching up, don't start this routine immediately.
	if memR.WaitSync() {
		select {
		case <-memR.waitSyncCh:
			// EnableInOutTxs() has set WaitSync() to false.
		case <-memR.Quit():
			return
		}
	}

	// TODO Candidate to remove if cc-based algorithm works
	sentTxs := make(map[types.TxKey]struct{})

	for {
		// In case of both next.NextWaitChan() and peer.Quit() are variable at the same time
		if !memR.IsRunning() || !peer.IsRunning() {
			return
		}

		// This happens because the CElement we were looking at got garbage
		// collected (removed). That is, .NextWait() returned nil. Go ahead and
		// start from the beginning.
		if next == nil {
			select {
			case <-memR.mempool.TxsWaitChan(): // Wait until a tx is available
				if next = memR.mempool.TxsFront(); next == nil {
					continue
				}
			case <-peer.Quit():
				return
			case <-memR.Quit():
				return
			}
		}

		memTx := next.Value.(*mempoolTx)
		txKey := memTx.tx.Key()
		txKeyString := txKey.String()
		senders := memTx.Senders()

		// Check if tx was sent before.
		if _, ok := sentTxs[txKey]; ok {
			memR.Logger.Debug("🚱 do not send tx again to the same peer! ", "tx-sender", senders, "target", peer.ID().ShortString(), "tx", txKeyString)
		} else if memR.router.areRoutesEnabled(senders, peer.ID()) { // Check if the route to this peer is enabled.
			// Make sure the peer is up to date.
			peerState, ok := peer.Get(types.PeerStateKey).(PeerState)
			if !ok {
				// Peer does not have a state yet. We set it in the consensus reactor, but
				// when we add peer in Switch, the order we call reactors#AddPeer is
				// different every time due to us using a map. Sometimes other reactors
				// will be initialized before the consensus reactor. We should wait a few
				// milliseconds and retry.
				time.Sleep(PeerCatchupSleepIntervalMS * time.Millisecond)
				continue
			}

			// If we suspect that the peer is lagging behind, at least by more than
			// one block, we don't send the transaction immediately. This code
			// reduces the mempool size and the recheck-tx rate of the receiving
			// node. See [RFC 103] for an analysis on this optimization.
			//
			// [RFC 103]: https://github.com/cometbft/cometbft/pull/735
			if peerState.GetHeight() < memTx.Height()-1 {
				time.Sleep(PeerCatchupSleepIntervalMS * time.Millisecond)
				continue
			}

			// NOTE: Transaction batching was disabled due to
			// https://github.com/tendermint/tendermint/issues/5796

			if !peer.Send(p2p.Envelope{ChannelID: MempoolChannel, Message: &protomem.Tx{Tx: memTx.tx, Nonce: memTx.nonce}}) {
				time.Sleep(PeerCatchupSleepIntervalMS * time.Millisecond)
				continue
			}
			memR.Logger.Debug("✅ Sent Tx message", "tx-sender", senders, "to", peer.ID().ShortString(), "tx", txKeyString)

			// mark tx as sent
			sentTxs[txKey] = struct{}{}
		} else {
			memR.Logger.Debug("🚫 do not send tx! ", "tx-sender", senders, "target", peer.ID().ShortString(), "tx", txKeyString)
		}

		select {
		case <-next.NextWaitChan():
			// see the start of the for loop for nil check
			next = next.Next()
		case <-peer.Quit():
			return
		case <-memR.Quit():
			return
		}
	}
}

type p2pIDSet = map[p2p.ID]struct{}

type gossipRouter struct {
	// A set of `source -> target` routes that are disabled for disseminating transactions. Source
	// and target are node IDs.
	disabledRoutes map[p2p.ID]p2pIDSet
	first          int64 // number of transactions received for the first time
	duplicate      int64 // number of duplicate transactions
	mtx            cmtsync.RWMutex

	blockHaveTx atomic.Bool
}

func newGossipRouter() *gossipRouter {
	return &gossipRouter{
		disabledRoutes: make(map[p2p.ID]p2pIDSet),
	}
}

func (r *gossipRouter) setBlockHaveTx() {
	r.blockHaveTx.Store(true)
}

func (r *gossipRouter) isHaveTxBlocked() bool {
	return r.blockHaveTx.Load()
}

func (r *gossipRouter) incDuplicateTxs() {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.duplicate++
}

const (
	targetRedundancy      = 1
	targetRedundancySlack = 10  // expressed as % of targetRedundancy
	txsPerAdjustment      = 500 // We probably need to make this bigger
)

func (r *gossipRouter) incFirstTimeTx() {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.first++
}

func (r *gossipRouter) adjustRedundancy(logger log.Logger) (float64, bool) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if r.first >= txsPerAdjustment {
		sendReset := false
		redundancy := float64(r.duplicate) / float64(r.first)
		targetRedundancySlackAbs := float64(targetRedundancy) * float64(targetRedundancySlack) / 100.
		if redundancy < targetRedundancy-targetRedundancySlackAbs {
			logger.Info("TX redundancy BELOW limit, increasing it",
				"redundancy", redundancy,
				"limit", targetRedundancy+targetRedundancySlackAbs,
			)
			sendReset = true
		} else if targetRedundancy+targetRedundancySlackAbs <= redundancy {
			logger.Info("TX redundancy ABOVE limit, decreasing it",
				"redundancy", redundancy,
				"limit", targetRedundancy-targetRedundancySlackAbs,
			)
			r.blockHaveTx.Store(false)
		}
		r.first = 0
		r.duplicate = 0
		return redundancy, sendReset
	}
	return -1, false
}

// disableRoute marks the route `source -> target` as disabled.
func (r *gossipRouter) disableRoute(source, target p2p.ID) {
	if source == noSender || target == noSender {
		// TODO: this shouldn't happen
		return
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()

	targets, ok := r.disabledRoutes[source]
	if !ok {
		targets = make(p2pIDSet)
	}
	targets[target] = struct{}{}
	r.disabledRoutes[source] = targets
}

// isRouteEnabled returns true iff the route source->target is enabled.
func (r *gossipRouter) isRouteEnabled(source, target p2p.ID) bool {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	// do not send to sender
	if source == target {
		return false
	}

	if targets, ok := r.disabledRoutes[source]; ok {
		for p := range targets {
			if p == target {
				return false
			}
		}
	}
	return true
}

// areRoutesEnabled returns false iff all the routes from the list of sources to target are disabled.
func (r *gossipRouter) areRoutesEnabled(sources []p2p.ID, target p2p.ID) bool {
	if len(sources) == 0 {
		return true
	}
	for _, s := range sources {
		if r.isRouteEnabled(s, target) {
			return true
		}
	}
	return false
}

// resetRoutes removes all disabled routes with peerID as source or target.
func (r *gossipRouter) resetRoutes(peerID p2p.ID) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	// remove peer as source
	delete(r.disabledRoutes, peerID)

	// remove peer as target
	for _, targets := range r.disabledRoutes {
		delete(targets, peerID)
	}
}

// numRoutes returns the number of disabled routes.
func (r *gossipRouter) numRoutes() int {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	count := 0
	for _, targets := range r.disabledRoutes {
		count += len(targets)
	}
	return count
}
