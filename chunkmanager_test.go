package txfun

import (
	"os"
	"path/filepath"
	"testing"
)

func TestChunkManager(t *testing.T) {
	t.Log(newChunkManager(filepath.Join(os.TempDir(), "chunks")))
}
