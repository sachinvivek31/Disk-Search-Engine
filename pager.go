package main

import (
	"errors"
	"io"
	"os"
)

const PAGE_SIZE = 4096

type Pager struct {
	file     *os.File
	fileSize int64
}

func NewPager(filename string) (*Pager, error) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}

	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	return &Pager{
		file:     f,
		fileSize: st.Size(),
	}, nil
}

func (p *Pager) GetPage(pageIndex uint32) ([]byte, error) {
	if p == nil || p.file == nil {
		return nil, errors.New("pager is not initialized")
	}

	offset := int64(pageIndex) * int64(PAGE_SIZE)
	if offset >= p.fileSize {
		return make([]byte, PAGE_SIZE), nil
	}

	if _, err := p.file.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}

	buf := make([]byte, PAGE_SIZE)
	n, err := io.ReadFull(p.file, buf)
	if err == nil {
		return buf, nil
	}

	// If we hit EOF mid-page, return the partial data with the remainder left as zeros.
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		_ = n
		return buf, nil
	}

	return nil, err
}

func (p *Pager) FlushPage(pageIndex uint32, data []byte) error {
	if p == nil || p.file == nil {
		return errors.New("pager is not initialized")
	}
	if len(data) != PAGE_SIZE {
		return errors.New("FlushPage requires exactly PAGE_SIZE bytes")
	}

	offset := int64(pageIndex) * int64(PAGE_SIZE)
	if _, err := p.file.Seek(offset, io.SeekStart); err != nil {
		return err
	}

	if _, err := p.file.Write(data); err != nil {
		return err
	}

	end := offset + int64(PAGE_SIZE)
	if end > p.fileSize {
		p.fileSize = end
	}

	return nil
}

// NextPageIndex returns the next available page index (for allocating new pages).
// Page 0 is reserved for the header, so the first data page is 1.
func (p *Pager) NextPageIndex() uint32 {
	if p.fileSize < int64(PAGE_SIZE) {
		return 1
	}
	return uint32(p.fileSize / int64(PAGE_SIZE))
}

