package txfun

type chunkreader struct {
	chunks []*chunk
}

type multichunkcursor struct {
	cursors []*chunkCursor
}
