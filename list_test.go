package txfun

import (
	"testing"
)

func TestList1(t *testing.T) {
	l := newList()
	l.insert([]byte("foo"), []byte("bar"))
	l.insert([]byte("a"), []byte("a"))
	l.insert([]byte{0}, []byte("null"))
	l.insert([]byte("foo\x00"), []byte("foonull"))

	t.Log(l)
}
