package txfun

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func TestChunk(t *testing.T) {
	const N = 1e3

	f, err := ioutil.TempFile(os.TempDir(), "chunk_")
	if err != nil {
		t.Fatal(err)
	}

	c := &chunk{
		file: f,
	}

	f2, err := ioutil.TempFile(os.TempDir(), "chunk_")
	if err != nil {
		t.Fatal(err)
	}

	c2 := &chunk{
		file: f2,
	}

	f3, err := ioutil.TempFile(os.TempDir(), "chunk_")
	if err != nil {
		t.Fatal(err)
	}

	c3 := &chunk{
		file: f3,
	}

	aList := newList()
	bList := newList()

	for i := 0; i < N; i++ {
		aList.insert([]byte(fmt.Sprint(i*2)), []byte(fmt.Sprint(N-i)))
		bList.insert([]byte(fmt.Sprint(i*2+1)), []byte(fmt.Sprint(N-i*2)))
	}

	for n := aList.root; n != nil; n = n.next {
		c.WriteRecord(createRecord(n.key, n.value))
	}

	for n := bList.root; n != nil; n = n.next {
		c2.WriteRecord(createRecord(n.key, n.value))
	}

	mergeChunks(c, c2, c3)

	cur := c3.NewCursor()
	for {
		rec := cur.readRecord()
		if rec.magic != RECORD_MAGIC {
			break
		}

		t.Log(string(rec.key), string(rec.value))

		cur.next()
	}
}
