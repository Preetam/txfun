package txfun

import (
	"bytes"
	"errors"
)

var (
	ErrNotFound = errors.New("tx: key not found")
	ErrConflict = errors.New("tx: conflict")
	ErrUnknown  = errors.New("unknown error")
)

type Tx struct {
	id          uint32
	db          *DB
	epoch       uint64
	state       *list
	conflicted  bool
	keysWritten map[string]struct{}
	commits     []map[string]struct{}
}

func (tx *Tx) Rollback() {
}

func (tx *Tx) Commit() error {
CONFLICT_CHECK:
	for _, committed := range tx.commits {
		for key := range committed {
			if _, present := tx.keysWritten[key]; present {
				tx.conflicted = true
				break CONFLICT_CHECK
			}
		}
	}
	return tx.db.commitTx(tx)
}

func (tx *Tx) Set(key, value []byte) error {
	tx.keysWritten[string(key)] = struct{}{}
	tx.state.insert(key, value)
	return nil
}

func (tx *Tx) Get(key []byte) ([]byte, error) {
	for n := tx.state.root; n != nil; n = n.next {
		cmp := bytes.Compare(key, n.key)
		switch {
		case cmp == 0:
			return n.value, nil
		case cmp > 0:
			break
		}
	}

	for n := tx.db.state.root; n != nil; n = n.next {
		if n.created > tx.epoch {
			continue
		}

		cmp := bytes.Compare(key, n.key)
		switch {
		case cmp == 0:
			return n.value, nil
		case cmp > 0:
			break
		}
	}

	return nil, ErrNotFound
}

func (tx *Tx) addCommits(commits ...map[string]struct{}) {
	tx.commits = append(tx.commits, commits...)
}
