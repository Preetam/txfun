package txfun

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"sync"
)

type chunkmanager struct {
	path   string
	chunks []*chunk
	lock   sync.Mutex
}

func newChunkManager(path string) (*chunkmanager, error) {
	err := os.MkdirAll(path, 0755)
	if err != nil {
		return nil, err
	}
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}

	var chunks []*chunk

	for _, fileInfo := range files {
		if name := fileInfo.Name(); strings.HasSuffix(name, ".chunk") {
			chunkEpochStr := strings.TrimSuffix(name, ".chunk")
			epoch := uint64(0)
			fmt.Sscanf(chunkEpochStr, "%d", &epoch)

			if epoch > 0 {
				fmt.Printf("Found chunk %d\n", epoch)
				chunk, err := openChunk(path, name)
				if err != nil {
					return nil, err
				}

				chunks = append(chunks, chunk)
			}
		}
	}

	sort.Sort(byEpoch(chunks))

	cm := &chunkmanager{
		path:   path,
		chunks: chunks,
		lock:   sync.Mutex{},
	}

	return cm, nil
}

func (cm *chunkmanager) String() string {
	str := ""
	str += fmt.Sprintf("chunks at %s:\n", cm.path)
	for _, chunk := range cm.chunks {
		str += fmt.Sprintf("chunk %d, writeable: %v\n", chunk.epoch, chunk.writeable)
	}
	return str
}

type byEpoch []*chunk

func (e byEpoch) Len() int           { return len(e) }
func (e byEpoch) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }
func (e byEpoch) Less(i, j int) bool { return e[i].epoch < e[j].epoch }
