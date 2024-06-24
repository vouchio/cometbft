package txhash

import (
	"crypto"
	"fmt"
	gohash "hash"
)

var (
	// Hash function used for transaction hashing.
	hash Hash = crypto.SHA256

	// fmt is a function that converts a byte slice to a string.
	fmtHash = func(bz []byte) string {
		return fmt.Sprintf("%X", bz)
	}
)

// Hash is an interface for transaction hashing.
type Hash interface {
	// New returns a new hash.Hash.
	New() gohash.Hash
}

// Bytes is a wrapper around a byte slice that implements the fmt.Stringer.
type Bytes []byte

func (bz Bytes) String() string {
	return fmtHash(bz)
}

func (bz Bytes) Unwrap() []byte {
	return bz
}

// Set sets the hash function used for transaction hashing.
//
// Call this function before starting the node. Changing the hashing function
// after the chain has started can ONLY be done with a hard fork.
func Set(h Hash) {
	hash = h
}

// SetFmtHash sets the function used to convert a checksum to a string.
func SetFmtHash(f func([]byte) string) {
	fmtHash = f
}

// Sum returns the checksum of the data as Bytes.
func Sum(bz []byte) Bytes {
	return Bytes(hash.New().Sum(bz))
}
