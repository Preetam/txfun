package txfun

import (
	"bytes"
	"encoding/binary"
	"os"
)

const (
	RECORD_MAGIC = 0x1114
)

type chunk struct {
	file *os.File
}

type chunkCursor struct {
	file *os.File
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
