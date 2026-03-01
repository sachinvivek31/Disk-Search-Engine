package main

import (
	"encoding/binary"
	"errors"
)

// User represents a user record with a fixed-size username field.
type User struct {
	ID        uint32
	Username  [32]byte
	IsDeleted bool
}

// Serialize converts a User into a 37-byte buffer using LittleEndian encoding.
func Serialize(u User) ([]byte, error) {
	buf := make([]byte, 37)

	// First 4 bytes: ID (uint32, LittleEndian)
	binary.LittleEndian.PutUint32(buf[0:4], u.ID)

	// Next 32 bytes: Username (fixed-size [32]byte)
	copy(buf[4:36], u.Username[:])

	// Byte 36: IsDeleted (1 = true, 0 = false)
	if u.IsDeleted {
		buf[36] = 1
	}

	return buf, nil
}

// Deserialize converts a 37-byte buffer into a User struct using LittleEndian encoding.
func Deserialize(buf []byte) (User, error) {
	if len(buf) < 37 {
		return User{}, errors.New("buffer too short for User deserialization, need 37 bytes")
	}

	var u User

	// First 4 bytes: ID
	u.ID = binary.LittleEndian.Uint32(buf[0:4])

	// Next 32 bytes: Username
	copy(u.Username[:], buf[4:36])

	// Byte 36: IsDeleted
	u.IsDeleted = buf[36] != 0

	return u, nil
}

