package txfun

import (
	"bytes"
)

type chunkreader struct {
	chunks []*chunk
}

type multichunkcursor struct {
	cursors []*chunkCursor
	key     []byte
	value   []byte
	valid   bool
}

func (c *chunkreader) newCursor() *multichunkcursor {
	cursors := []*chunkCursor{}
	for _, chunk := range c.chunks {
		cursors = append(cursors, chunk.NewCursor())
	}

	valid := false
	if len(cursors) > 0 {
		valid = true
	}

	return &multichunkcursor{
		cursors: cursors,
		valid:   valid,
	}
}

func (m *multichunkcursor) Key() []byte {
	return m.key
}

func (m *multichunkcursor) Value() []byte {
	return m.value
}

func (m *multichunkcursor) Next() bool {
	minIndex := -1
	minKey := []byte(nil)
	minValue := []byte(nil)

	// find min
	for i, cur := range m.cursors {
		if cur.valid {
			if minKey == nil {
				minKey = cur.key
				minIndex = i
				continue
			}

			if bytes.Compare(cur.key, minKey) < 0 {
				minKey = cur.key
				minIndex = i
				continue
			}
		}
	}

	if minKey == nil {
		m.valid = false
		return false
	}

	m.cursors[minIndex].next()
	m.key = minKey
	m.value = minValue

	return m.valid
}

func (m *multichunkcursor) Valid() bool {
	return m.valid
}
