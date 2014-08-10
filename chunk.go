package txfun

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	RECORD_MAGIC = 0x1114
)

type chunk struct {
	epoch     uint64
	writeable bool
	file      *os.File
}

func openChunk(path, file string) (*chunk, error) {
	chunkEpochStr := strings.TrimSuffix(file, ".chunk")
	epoch := uint64(0)
	fmt.Sscanf(chunkEpochStr, "%d", &epoch)
	f, err := os.Open(filepath.Join(path, file))
	if err != nil {
		return nil, err
	}

	return &chunk{
		epoch:     epoch,
		file:      f,
		writeable: false,
	}, nil
}

func createChunk(path string, epoch uint64) (*chunk, error) {
	filename := filepath.Join(path, fmt.Sprintf("%d.chunk", epoch))
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	return &chunk{
		epoch:     epoch,
		file:      f,
		writeable: true,
	}, nil
}

type chunkCursor struct {
	file  *os.File
	key   []byte
	value []byte
	valid bool
}

type record struct {
	magic  uint16
	keylen uint16
	vallen uint32
	key    []byte
	value  []byte
}

func createRecord(key, value []byte) record {
	return record{
		magic:  RECORD_MAGIC,
		keylen: uint16(len(key)),
		vallen: uint32(len(value)),
		key:    key,
		value:  value,
	}
}

func (c *chunk) WriteRecord(r record) error {
	err := binary.Write(c.file, binary.BigEndian, r.magic)
	if err != nil {
		return err
	}
	err = binary.Write(c.file, binary.BigEndian, r.keylen)
	if err != nil {
		return err
	}
	err = binary.Write(c.file, binary.BigEndian, r.vallen)
	if err != nil {
		return err
	}

	_, err = c.file.Write(r.key)
	if err != nil {
		return err
	}

	_, err = c.file.Write(r.value)
	if err != nil {
		return err
	}

	return nil
}

func (c *chunk) NewCursor() *chunkCursor {
	f, _ := os.Open(c.file.Name())

	cur := &chunkCursor{
		file: f,
	}

	cur.file.Seek(0, 0)
	cur.readRecord()

	return cur
}

func (cur *chunkCursor) readRecord() record {
	rec := record{}
	binary.Read(cur.file, binary.BigEndian, &rec.magic)
	binary.Read(cur.file, binary.BigEndian, &rec.keylen)
	binary.Read(cur.file, binary.BigEndian, &rec.vallen)
	rec.key = make([]byte, int(rec.keylen))
	rec.value = make([]byte, int(rec.vallen))
	cur.file.Read(rec.key)
	cur.file.Read(rec.value)
	cur.file.Seek(-int64(len(rec.key)+len(rec.value)+8), 1)

	cur.key = rec.key
	cur.value = rec.value
	cur.valid = rec.magic == RECORD_MAGIC

	return rec
}

func (cur *chunkCursor) next() {
	cur.file.Seek(8+int64(len(cur.key)+len(cur.value)), 1)
	cur.readRecord()
}

func mergeChunks(a, b, dest *chunk) {
	aCur := a.NewCursor()
	bCur := b.NewCursor()

	for {
		if !aCur.valid {
			if !bCur.valid {
				break
			}

			dest.WriteRecord(createRecord(bCur.key, bCur.value))
			bCur.next()

			continue
		}

		if !bCur.valid {
			dest.WriteRecord(createRecord(aCur.key, aCur.value))
			aCur.next()

			continue
		}

		cmp := bytes.Compare(aCur.key, bCur.key)
		switch {
		case cmp < 0:
			dest.WriteRecord(createRecord(aCur.key, aCur.value))
			aCur.next()
		case cmp > 0:
			dest.WriteRecord(createRecord(bCur.key, bCur.value))
			bCur.next()
		case cmp == 0:
			dest.WriteRecord(createRecord(bCur.key, bCur.value))
			aCur.next()
			bCur.next()
		}
	}
}
