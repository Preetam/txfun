package txfun

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func Test1(t *testing.T) {
	db, _ := NewDB()
	tx := db.Begin()

	tx.Set([]byte("foo"), []byte("bar"))
	tx.Get([]byte("foo"))

	tx.Commit()

	tx = db.Begin()
	val, err := tx.Get([]byte("foo"))
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(val))
	tx.Set([]byte("foo"), append(val, []byte("baz")...))
	t.Log(db.state)
	tx.Commit()
	t.Log(db.state)

	txA := db.Begin()
	txB := db.Begin()

	t.Log(db.state)
	txA.Set([]byte("some_key"), []byte("a"))
	txA.Commit()
	t.Log(db.state)
	txB.Set([]byte("some_key"), []byte("b"))
	t.Log(db.state)
	t.Log(txB.Commit())
	t.Log(db.state)
	t.Log(txB.Commit())
	t.Log(db.state)
}

func TestConcurrent(t *testing.T) {
	const N = 50 // # goroutines
	var cases []map[string]bool
	keys := map[string]bool{}

	for i := 0; i < N; i++ {
		m := map[string]bool{}
		for j := 0; j < 50; j++ {
			str := fmt.Sprint(rand.Int())
			m[str] = false
			keys[str] = false
		}

		cases = append(cases, m)
	}

	db, _ := NewDB()

	wg := sync.WaitGroup{}

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(n int) {
			tx := db.Begin()
			t.Logf("[tx %d] starting tx", tx.id)

			for key := range cases[n] {
				tx.Set([]byte(key), []byte(fmt.Sprint(n)))
			}

			txStart := time.Now()
			for err := tx.Commit(); err != nil; err = tx.Commit() {
				t.Logf("[tx %d] retrying", tx.id)
				txStart = time.Now()
			}
			t.Logf("[tx %d] took %v to commit", tx.id, time.Now().Sub(txStart))
			wg.Done()
		}(i)
	}

	wg.Wait()

	for n := db.state.root; n != nil; n = n.next {
		keys[string(n.key)] = true
	}

	for key, val := range keys {
		if !val {
			t.Errorf("key %v was not found", key)
		}
	}
}
