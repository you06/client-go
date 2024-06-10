package art

import (
	"github.com/tikv/client-go/v2/kv"
)

var tombstone = []byte{}

type tree struct {
	root *artNode
}

func New() *tree {
	return &tree{}
}

func (t *tree) Set(key, value []byte) error {
	return t.set(key, value)
}

func (t *tree) Delete(key []byte) error {
	return t.set(key, tombstone)
}

func (t *tree) set(key, value []byte, ops ...kv.FlagsOp) error {
	return nil
}

func (t *tree) Get(key []byte) ([]byte, error) {
	return nil, nil
}
