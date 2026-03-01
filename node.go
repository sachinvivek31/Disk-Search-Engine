package main

import (
	"encoding/binary"
	"log"
)

// NodeType represents the type of a B-tree node.
type NodeType uint8

const (
	NODE_INTERNAL NodeType = iota
	NODE_LEAF
)

// Leaf node layout (after the 6-byte NodeHeader).
const (
	LEAF_NODE_NUM_CELLS_OFFSET = 6
	LEAF_NODE_HEADER_SIZE     = 10
	LEAF_NODE_CELL_SIZE       = 41 // 4 bytes key + 37 bytes User data
	LEAF_NODE_MAX_CELLS       = (PAGE_SIZE - LEAF_NODE_HEADER_SIZE) / LEAF_NODE_CELL_SIZE
)

// Internal node layout (after the 6-byte NodeHeader).
const (
	INTERNAL_NODE_NUM_KEYS_OFFSET   = 6
	INTERNAL_NODE_RIGHT_CHILD_OFFSET = 10
	INTERNAL_NODE_HEADER_SIZE       = 14
	INTERNAL_NODE_CELL_SIZE         = 8 // 4 bytes Key + 4 bytes Child Page ID
)

// NodeHeader represents the header stored at the beginning of each node page.
type NodeHeader struct {
	Type          NodeType
	IsRoot        bool
	ParentPointer uint32
}

// ReadNodeHeader reads a NodeHeader from the start of a 4096-byte page buffer.
func ReadNodeHeader(page []byte) NodeHeader {
	if len(page) < 6 {
		// Return zero value if page is too small
		return NodeHeader{}
	}

	return NodeHeader{
		Type:          NodeType(page[0]),
		IsRoot:        page[1] != 0,
		ParentPointer: binary.LittleEndian.Uint32(page[2:6]),
	}
}

// WriteNodeHeader writes a NodeHeader to the start of a 4096-byte page buffer.
func WriteNodeHeader(page []byte, header NodeHeader) {
	if len(page) < 6 {
		return
	}

	page[0] = byte(header.Type)
	if header.IsRoot {
		page[1] = 1
	} else {
		page[1] = 0
	}
	binary.LittleEndian.PutUint32(page[2:6], header.ParentPointer)
}

// LeafNodeNumCells reads the cell count from the leaf node header.
func LeafNodeNumCells(page []byte) uint32 {
	if len(page) < LEAF_NODE_HEADER_SIZE {
		return 0
	}
	return binary.LittleEndian.Uint32(page[LEAF_NODE_NUM_CELLS_OFFSET:LEAF_NODE_HEADER_SIZE])
}

// LeafNodeCell returns a slice pointing to the cell at cellNum (length LEAF_NODE_CELL_SIZE).
func LeafNodeCell(node []byte, cellNum uint32) []byte {
    // Offset = Header Size + (Cell Number * Row Size)
    offset := LEAF_NODE_HEADER_SIZE + (cellNum * LEAF_NODE_CELL_SIZE)
    
    // SAFETY CHECK: If offset is bigger than 4096, the data is corrupt
    if offset >= 4096 {
        log.Panicf("Corrupt Node! Trying to access offset %d", offset)
    }
    return node[offset:]
}

// leafNodeSetNumCells writes the cell count into the leaf node header.
func leafNodeSetNumCells(page []byte, count uint32) {
	binary.LittleEndian.PutUint32(page[LEAF_NODE_NUM_CELLS_OFFSET:LEAF_NODE_HEADER_SIZE], count)
}

// LeafNodeInsert inserts or overwrites a key/value in the leaf node (sorted by key).
func LeafNodeInsert(page []byte, key uint32, value []byte) {
	numCells := LeafNodeNumCells(page)

	// Find the index where key belongs (binary search)
	lo, hi := uint32(0), numCells
	for lo < hi {
		mid := lo + (hi-lo)/2
		cell := LeafNodeCell(page, mid)
		cellKey := binary.LittleEndian.Uint32(cell[0:4])
		if cellKey < key {
			lo = mid + 1
		} else if cellKey > key {
			hi = mid
		} else {
			// Key exists: overwrite value (bytes 4..40)
			copy(cell[4:4+LEAF_NODE_CELL_SIZE-4], value)
			return
		}
	}

	// New key: insert at index lo. Shift cells [lo, numCells) right by one (copy backwards to avoid overwriting).
	for i := numCells; i > lo; i-- {
		copy(LeafNodeCell(page, i), LeafNodeCell(page, i-1))
	}
	insertOffset := LEAF_NODE_HEADER_SIZE + lo*LEAF_NODE_CELL_SIZE

	// Write the new cell
	cell := page[insertOffset : insertOffset+LEAF_NODE_CELL_SIZE]
	binary.LittleEndian.PutUint32(cell[0:4], key)
	copy(cell[4:], value)

	leafNodeSetNumCells(page, numCells+1)
}

// InternalNodeNumKeys reads the key count from the internal node header.
func InternalNodeNumKeys(page []byte) uint32 {
	if len(page) < INTERNAL_NODE_HEADER_SIZE {
		return 0
	}
	return binary.LittleEndian.Uint32(page[INTERNAL_NODE_NUM_KEYS_OFFSET:INTERNAL_NODE_RIGHT_CHILD_OFFSET])
}

// InternalNodeRightChild reads the rightmost child page pointer.
func InternalNodeRightChild(page []byte) uint32 {
	if len(page) < INTERNAL_NODE_HEADER_SIZE {
		return 0
	}
	return binary.LittleEndian.Uint32(page[INTERNAL_NODE_RIGHT_CHILD_OFFSET:INTERNAL_NODE_HEADER_SIZE])
}

// InternalNodeCell returns a slice pointing to the cell at cellNum (length INTERNAL_NODE_CELL_SIZE).
func InternalNodeCell(page []byte, cellNum uint32) []byte {
	offset := INTERNAL_NODE_HEADER_SIZE + cellNum*INTERNAL_NODE_CELL_SIZE
	return page[offset : offset+INTERNAL_NODE_CELL_SIZE]
}

func internalNodeSetNumKeys(page []byte, count uint32) {
	binary.LittleEndian.PutUint32(page[INTERNAL_NODE_NUM_KEYS_OFFSET:INTERNAL_NODE_RIGHT_CHILD_OFFSET], count)
}

func internalNodeSetRightChild(page []byte, child uint32) {
	binary.LittleEndian.PutUint32(page[INTERNAL_NODE_RIGHT_CHILD_OFFSET:INTERNAL_NODE_HEADER_SIZE], child)
}

// InternalNodeInsert adds a key with left and right children after a split.
// Cell format: (key, leftChildPage) and right_child = rightChildPage.
func InternalNodeInsert(parentPage []byte, key uint32, leftChildPage uint32, rightChildPage uint32) {
	numKeys := InternalNodeNumKeys(parentPage)

	// Find the index where key belongs (binary search)
	lo, hi := uint32(0), numKeys
	for lo < hi {
		mid := lo + (hi-lo)/2
		cell := InternalNodeCell(parentPage, mid)
		cellKey := binary.LittleEndian.Uint32(cell[0:4])
		if cellKey < key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	// Shift cells [lo, numKeys) right by one
	for i := numKeys; i > lo; i-- {
		copy(InternalNodeCell(parentPage, i), InternalNodeCell(parentPage, i-1))
	}

	// Write the new cell (key, leftChildPage)
	cell := InternalNodeCell(parentPage, lo)
	binary.LittleEndian.PutUint32(cell[0:4], key)
	binary.LittleEndian.PutUint32(cell[4:8], leftChildPage)

	internalNodeSetRightChild(parentPage, rightChildPage)
	internalNodeSetNumKeys(parentPage, numKeys+1)
}

// InternalNodeFindChild returns the child page index for the given id.
// Cell format: (key, leftChildPage) where leftChildPage has keys < key.
// right_child has keys >= last key.
func InternalNodeFindChild(page []byte, id uint32) uint32 {
	numKeys := InternalNodeNumKeys(page)
	for i := uint32(0); i < numKeys; i++ {
		cell := InternalNodeCell(page, i)
		cellKey := binary.LittleEndian.Uint32(cell[0:4])
		if id < cellKey {
			return binary.LittleEndian.Uint32(cell[4:8])
		}
	}
	return InternalNodeRightChild(page)
}

// LeafNodeFind returns the 36-byte value for the given id, or nil if not found.
func LeafNodeFind(page []byte, id uint32) []byte {
	numCells := LeafNodeNumCells(page)
	lo, hi := uint32(0), numCells
	for lo < hi {
		mid := lo + (hi-lo)/2
		cell := LeafNodeCell(page, mid)
		cellKey := binary.LittleEndian.Uint32(cell[0:4])
		if cellKey < id {
			lo = mid + 1
		} else if cellKey > id {
			hi = mid
		} else {
			return cell[4 : 4+37]
		}
	}
	return nil
}

// LeafNodeMarkDeleted sets the IsDeleted byte to true for the cell with the given id.
// Returns true if the cell was found and marked.
func LeafNodeMarkDeleted(page []byte, id uint32) bool {
	numCells := LeafNodeNumCells(page)
	lo, hi := uint32(0), numCells
	for lo < hi {
		mid := lo + (hi-lo)/2
		cell := LeafNodeCell(page, mid)
		cellKey := binary.LittleEndian.Uint32(cell[0:4])
		if cellKey < id {
			lo = mid + 1
		} else if cellKey > id {
			hi = mid
		} else {
			// IsDeleted is byte 36 of the value (byte 40 of the cell)
			cell[4+36] = 1
			return true
		}
	}
	return false
}
