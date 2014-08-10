package txfun

import (
	"os"
	"path/filepath"
	"testing"
)

func TestChunkManager(t *testing.T) {
	cm, err := newChunkManager(filepath.Join(os.TempDir(), "chunks"))
	if err != nil {
		t.Fatal(err)
	}

	reader := cm.newChunkReader(2)
	cur := reader.newCursor()

	for cur.Next() {
		t.Log(string(cur.key), string(cur.value))
	}
}
