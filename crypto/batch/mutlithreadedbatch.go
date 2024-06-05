package batch

import (
	"github.com/cometbft/cometbft/crypto"
)

type batchVerifierRequest struct {
	verify bool
	key    crypto.PubKey
	msg    []byte
	sig    []byte
}

type threadedBatchVerifier struct {
	batchVerificationChans []chan *batchVerifierRequest
	verifyResponseChan     chan bool
	numThreads             int
	curThreadIndex         int
}

type batchVerifierThread struct {
	bv       crypto.BatchVerifier
	addError error

	communicationChan  chan *batchVerifierRequest
	verifyResponseChan chan bool
}

// NewThreadedBatchVerifier creates a new threaded batch verifier.
func NewThreadedBatchVerifier(pk crypto.PubKey, numThreads int) (crypto.BatchVerifier, bool) {
	if numThreads <= 1 {
		return CreateBatchVerifier(pk)
	} else if !SupportsBatchVerifier(pk) {
		return nil, false
	}
	bv := &threadedBatchVerifier{
		numThreads: numThreads,
	}
	bv.batchVerificationChans = make([]chan *batchVerifierRequest, numThreads)
	verifyResponseChan := make(chan bool, numThreads)
	for i := 0; i < numThreads; i++ {
		bv.batchVerificationChans[i] = make(chan *batchVerifierRequest, 2)
		verifier, _ := CreateBatchVerifier(pk)
		batchVerifierThread := &batchVerifierThread{
			bv:                 verifier,
			communicationChan:  bv.batchVerificationChans[i],
			verifyResponseChan: verifyResponseChan,
		}
		go batchVerifierThread.listen()
	}
	return bv, true
}

func (tbv *threadedBatchVerifier) Add(key crypto.PubKey, msg []byte, sig []byte) error {
	request := &batchVerifierRequest{
		key: key,
		msg: msg,
		sig: sig,
	}
	tbv.batchVerificationChans[tbv.curThreadIndex] <- request
	tbv.curThreadIndex = (tbv.curThreadIndex + 1) % tbv.numThreads
	return nil
}

func (tbv *threadedBatchVerifier) Verify() (bool, []bool) {
	// Signal all threads to start verification
	request := &batchVerifierRequest{verify: true}
	for i := 0; i < tbv.numThreads; i++ {
		tbv.batchVerificationChans[i] <- request
	}
	allSuccess := true
	for i := 0; i < tbv.numThreads; i++ {
		success := <-tbv.verifyResponseChan
		if !success {
			allSuccess = false
		}
	}
	// TODO: Fix second arg
	return allSuccess, nil
}

func (bvt *batchVerifierThread) listen() {
	for {
		request := <-bvt.communicationChan
		if request.verify {
			if bvt.addError != nil {
				bvt.verifyResponseChan <- false
			} else {
				success, _ := bvt.bv.Verify()
				bvt.verifyResponseChan <- success
			}
			close(bvt.communicationChan)
			return
		} else {
			err := bvt.bv.Add(request.key, request.msg, request.sig)
			if err != nil {
				bvt.addError = err
			}
		}
	}
}
