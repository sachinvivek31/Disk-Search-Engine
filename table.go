package main

import (
	"encoding/binary"
	"sync"
)

const (
	ROW_SIZE      = 37
	ROWS_PER_PAGE = 110 // (PAGE_SIZE) / ROW_SIZE for heap layout
)

type Table struct {
	mu       sync.RWMutex
	Pager    *Pager
	NumRows  uint32
	RootPage uint32
}

// All Pager file operations (GetPage, FlushPage) are invoked only through Table methods.
// The Table mutex (RLock for reads, Lock for writes) serializes access and protects the Pager.

// NewTable creates a new Table and loads NumRows and RootPage from page 0.
func NewTable(p *Pager) (*Table, error) {
	table := &Table{
		Pager:    p,
		NumRows:  0,
		RootPage: 0,
	}

	// Read page 0 to get NumRows and RootPage from the header
	page0, err := p.GetPage(0)
	if err != nil {
		return nil, err
	}

	// If file is new (all zeros), NumRows and RootPage stay 0
	// Otherwise, decode the header bytes
	if p.fileSize >= 8 {
		// Bytes 0-3: NumRows
		table.NumRows = binary.LittleEndian.Uint32(page0[0:4])
		// Bytes 4-7: RootPage
		table.RootPage = binary.LittleEndian.Uint32(page0[4:8])
	}

	return table, nil
}

// RowSlot calculates the page index and byte offset for a given row number.
// Page 0 is reserved for metadata (NumRows), so rows start at page 1.
func (t *Table) RowSlot(rowNum uint32) (pageIndex uint32, offset uint32) {
	pageIndex = (rowNum / ROWS_PER_PAGE) + 1
	offset = (rowNum % ROWS_PER_PAGE) * ROW_SIZE
	return pageIndex, offset
}

// InsertRow inserts a User into the table using the B-tree index.
func (t *Table) InsertRow(u User) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	rowData, err := Serialize(u)
	if err != nil {
		return err
	}
	key := u.ID

	// Create root leaf if this is the first insert
	if t.RootPage == 0 {
		rootPage := t.Pager.NextPageIndex()
		page, err := t.Pager.GetPage(rootPage)
		if err != nil {
			return err
		}
		WriteNodeHeader(page, NodeHeader{Type: NODE_LEAF, IsRoot: true, ParentPointer: 0})
		LeafNodeInsert(page, key, rowData)
		if err := t.Pager.FlushPage(rootPage, page); err != nil {
			return err
		}
		t.RootPage = rootPage
		t.NumRows = 1
		return t.flushHeader()
	}

	leafPage := t.getLeafPageForInsert(key)
	page, err := t.Pager.GetPage(leafPage)
	if err != nil {
		return err
	}

	if LeafNodeNumCells(page) == LEAF_NODE_MAX_CELLS {
		return t.splitAndInsert(leafPage, key, rowData)
	}

	LeafNodeInsert(page, key, rowData)
	t.NumRows++
	if err := t.Pager.FlushPage(leafPage, page); err != nil {
		return err
	}
	return t.flushHeader()
}

// getLeafPageForInsert returns the leaf page where key should be inserted.
func (t *Table) getLeafPageForInsert(key uint32) uint32 {
	page, _ := t.Pager.GetPage(t.RootPage)
	header := ReadNodeHeader(page)
	if header.Type == NODE_LEAF {
		return t.RootPage
	}
	// Internal node: find child (for single-level tree)
	numKeys := InternalNodeNumKeys(page)
	for i := uint32(0); i < numKeys; i++ {
		cell := InternalNodeCell(page, i)
		cellKey := binary.LittleEndian.Uint32(cell[0:4])
		if key < cellKey {
			return binary.LittleEndian.Uint32(cell[4:8])
		}
	}
	return InternalNodeRightChild(page)
}

// splitAndInsert splits the leaf at leafPage, creates new root if needed, and inserts key/value.
func (t *Table) splitAndInsert(leafPage uint32, key uint32, value []byte) error {
	page, err := t.Pager.GetPage(leafPage)
	if err != nil {
		return err
	}

	// a) Create new page for right half
	newLeafPageIndex := t.Pager.NextPageIndex()
	newLeafPage, err := t.Pager.GetPage(newLeafPageIndex)
	if err != nil {
		return err
	}
	WriteNodeHeader(newLeafPage, NodeHeader{Type: NODE_LEAF, IsRoot: false, ParentPointer: 0})

	// b) Move right half to new page (split at 50: left keeps 0-49, right gets 50 to LEAF_NODE_MAX_CELLS-1)
	splitAt := uint32(50)
	for i := splitAt; i < LEAF_NODE_MAX_CELLS; i++ {
		cell := LeafNodeCell(page, i)
		cellKey := binary.LittleEndian.Uint32(cell[0:4])
		LeafNodeInsert(newLeafPage, cellKey, cell[4:4+ROW_SIZE])
	}
	leafNodeSetNumCells(page, splitAt)

	// Write headers for both leaves
	oldHeader := ReadNodeHeader(page)
	WriteNodeHeader(newLeafPage, NodeHeader{Type: NODE_LEAF, IsRoot: false, ParentPointer: 0})
	WriteNodeHeader(page, NodeHeader{Type: NODE_LEAF, IsRoot: oldHeader.IsRoot, ParentPointer: oldHeader.ParentPointer})

	promotedKey := binary.LittleEndian.Uint32(LeafNodeCell(page, splitAt)[0:4])

	// c) If this was the Root, create a new Internal Node to be the new Root
	if oldHeader.IsRoot {
		newRootPageIndex := t.Pager.NextPageIndex()
		newRootPage, err := t.Pager.GetPage(newRootPageIndex)
		if err != nil {
			return err
		}
		WriteNodeHeader(newRootPage, NodeHeader{Type: NODE_INTERNAL, IsRoot: true, ParentPointer: 0})
		InternalNodeInsert(newRootPage, promotedKey, leafPage, newLeafPageIndex)

		// Update parent pointers
		WriteNodeHeader(page, NodeHeader{Type: NODE_LEAF, IsRoot: false, ParentPointer: newRootPageIndex})
		WriteNodeHeader(newLeafPage, NodeHeader{Type: NODE_LEAF, IsRoot: false, ParentPointer: newRootPageIndex})

		t.RootPage = newRootPageIndex
		if err := t.Pager.FlushPage(newRootPageIndex, newRootPage); err != nil {
			return err
		}
	}

	// Flush both leaves
	if err := t.Pager.FlushPage(leafPage, page); err != nil {
		return err
	}
	if err := t.Pager.FlushPage(newLeafPageIndex, newLeafPage); err != nil {
		return err
	}

	// Insert the new row into the appropriate leaf
	if key < promotedKey {
		page, _ = t.Pager.GetPage(leafPage)
		LeafNodeInsert(page, key, value)
		t.Pager.FlushPage(leafPage, page)
	} else {
		newLeafPage, _ = t.Pager.GetPage(newLeafPageIndex)
		LeafNodeInsert(newLeafPage, key, value)
		t.Pager.FlushPage(newLeafPageIndex, newLeafPage)
	}

	t.NumRows++

	// d) Update Table.RootPage and save to Page 0
	return t.flushHeader()
}

func (t *Table) flushHeader() error {
	headerPage, err := t.Pager.GetPage(0)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(headerPage[0:4], t.NumRows)
	binary.LittleEndian.PutUint32(headerPage[4:8], t.RootPage)
	return t.Pager.FlushPage(0, headerPage)
}

// DeleteUser marks a user as deleted (tombstone). Does not decrement NumRows.
func (t *Table) DeleteUser(id uint32) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.RootPage == 0 {
		return nil
	}
	leafPage := t.getLeafPageForInsert(id)
	page, err := t.Pager.GetPage(leafPage)
	if err != nil {
		return err
	}
	if !LeafNodeMarkDeleted(page, id) {
		return nil // User not found, nothing to do
	}
	return t.Pager.FlushPage(leafPage, page)
}

// FindUser looks up a user by ID using the B-tree index.
func (t *Table) FindUser(id uint32) (*User, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.RootPage == 0 {
		return nil, nil
	}
	pageIndex := t.RootPage
	for {
		page, err := t.Pager.GetPage(pageIndex)
		if err != nil {
			return nil, err
		}
		header := ReadNodeHeader(page)
		if header.Type == NODE_INTERNAL {
			pageIndex = InternalNodeFindChild(page, id)
			continue
		}
		// LEAF node
		value := LeafNodeFind(page, id)
		if value == nil {
			return nil, nil
		}
		u, err := Deserialize(value)
		if err != nil {
			return nil, err
		}
		if u.IsDeleted {
			return nil, nil
		}
		return &u, nil
	}
}

// SelectAll reads all rows from the table by traversing the B-tree and returns them as a slice of User.
func (t *Table) SelectAll() ([]User, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.RootPage == 0 {
		return nil, nil
	}
	return t.collectUsersFromNode(t.RootPage)
}

func (t *Table) collectUsersFromNode(pageIndex uint32) ([]User, error) {
	page, err := t.Pager.GetPage(pageIndex)
	if err != nil {
		return nil, err
	}
	header := ReadNodeHeader(page)
	if header.Type == NODE_LEAF {
		users := make([]User, 0)
		numCells := LeafNodeNumCells(page)
		for i := uint32(0); i < numCells; i++ {
			cell := LeafNodeCell(page, i)
			u, err := Deserialize(cell[4 : 4+ROW_SIZE])
			if err != nil {
				return nil, err
			}
			if u.IsDeleted {
				continue
			}
			users = append(users, u)
		}
		return users, nil
	}
	// Internal node: recurse into children
	users := make([]User, 0)
	numKeys := InternalNodeNumKeys(page)
	for i := uint32(0); i < numKeys; i++ {
		cell := InternalNodeCell(page, i)
		childPage := binary.LittleEndian.Uint32(cell[4:8])
		childUsers, err := t.collectUsersFromNode(childPage)
		if err != nil {
			return nil, err
		}
		users = append(users, childUsers...)
	}
	rightChild := InternalNodeRightChild(page)
	rightUsers, err := t.collectUsersFromNode(rightChild)
	if err != nil {
		return nil, err
	}
	return append(users, rightUsers...), nil
}
