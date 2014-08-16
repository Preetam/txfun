package txfun

import (
	"math/rand"
	"sync"
	"sync/atomic"
)

type DB struct {
	state    *list
	lock     sync.Mutex
	epoch    uint64
	inflight map[uint32]*Tx
}

func NewDB() (*DB, error) {
	return &DB{
		state:    newList(),
		lock:     sync.Mutex{},
		epoch:    1,
		inflight: make(map[uint32]*Tx),
	}, nil
}

func (db *DB) Begin() *Tx {
	db.lock.Lock()
	defer db.lock.Unlock()

	id := rand.Uint32()

	for _, present := db.inflight[id]; present; id = rand.Uint32() {
	}

	tx := &Tx{
		id:          id,
		db:          db,
		epoch:       db.epoch,
		state:       newList(),
		keysWritten: make(map[string]struct{}),
	}

	db.inflight[id] = tx
	return tx
}

func (db *DB) commitTx(tx *Tx) error {
	if tx.conflicted {
		tx.epoch = db.epoch
		tx.conflicted = false
		tx.commits = tx.commits[:0]
		return ErrConflict
	}

	db.lock.Lock()

	for n := tx.state.root; n != nil; n = n.next {
		newNode := db.state.insert(n.key, n.value)
		newNode.created = db.epoch
	}

	db.lock.Unlock()

	for id, inflightTx := range db.inflight {
		if id == tx.id {
			continue
		}

		inflightTx.addCommits(tx.keysWritten)
	}

	atomic.AddUint64(&db.epoch, 1)
	delete(db.inflight, tx.id)

	return nil
}
