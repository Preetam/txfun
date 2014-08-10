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
	fmt.Println(chunkEpochStr)
	fmt.Sscanf(chunkEpochStr, "%d", &epoch)
	f, err := os.Open(filepath.Join(path, file))
	if err != nil {
		return nil, err
	}
	fmt.Println(epoch)
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

	return rec
}

func (cur *chunkCursor) next() {
	rec := record{}
	binary.Read(cur.file, binary.BigEndian, &rec.magic)
	binary.Read(cur.file, binary.BigEndian, &rec.keylen)
	binary.Read(cur.file, binary.BigEndian, &rec.vallen)
	cur.file.Seek(int64(rec.keylen)+int64(rec.vallen), 1)
}

func mergeChunks(a, b, dest *chunk) {
	aCur := a.NewCursor()
	bCur := b.NewCursor()

	for {
		aRec := aCur.readRecord()
		bRec := bCur.readRecord()

		if aRec.magic != RECORD_MAGIC {
			if bRec.magic != RECORD_MAGIC {
				break
			}

			dest.WriteRecord(createRecord(bRec.key, bRec.value))
			bCur.next()

			continue
		}

		if bRec.magic != RECORD_MAGIC {
			dest.WriteRecord(createRecord(aRec.key, aRec.value))
			aCur.next()

			continue
		}

		cmp := bytes.Compare(aRec.key, bRec.key)
		switch {
		case cmp < 0:
			dest.WriteRecord(createRecord(aRec.key, aRec.value))
			aCur.next()
		case cmp > 0:
			dest.WriteRecord(createRecord(bRec.key, bRec.value))
			bCur.next()
		case cmp == 0:
			dest.WriteRecord(createRecord(bRec.key, bRec.value))
			aCur.next()
			bCur.next()
		}
	}
}
