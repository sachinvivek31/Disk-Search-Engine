package main

import (
	"bytes"
	"os"
	"sync"
	"testing"
)

func setupBenchTable(tb testing.TB, n int) (*Table, func()) {
	f, err := os.CreateTemp("", "bench_*.db")
	if err != nil {
		tb.Fatal(err)
	}
	name := f.Name()
	f.Close()

	pager, err := NewPager(name)
	if err != nil {
		tb.Fatal(err)
	}
	table, err := NewTable(pager)
	if err != nil {
		pager.file.Close()
		tb.Fatal(err)
	}

	for i := uint32(0); i < uint32(n); i++ {
		var username [32]byte
		copy(username[:], []byte("user"))
		u := User{ID: i + 1, Username: username}
		if err := table.InsertRow(u); err != nil {
			pager.file.Close()
			tb.Fatal(err)
		}
	}

	cleanup := func() {
		pager.file.Close()
		os.Remove(name)
	}
	return table, cleanup
}

func BenchmarkSelectAll(b *testing.B) {
	table, cleanup := setupBenchTable(b, 1000)
	defer cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = table.SelectAll()
	}
}

func BenchmarkFindUser(b *testing.B) {
	table, cleanup := setupBenchTable(b, 1000)
	defer cleanup()

	targetID := uint32(500)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = table.FindUser(targetID)
	}
}

// TestFindUser verifies FindUser returns correct data
func TestFindUser(t *testing.T) {
	table, cleanup := setupBenchTable(t, 10)
	defer cleanup()

	u, err := table.FindUser(5)
	if err != nil {
		t.Fatal(err)
	}
	if u == nil {
		t.Fatal("expected user 5, got nil")
	}
	if u.ID != 5 {
		t.Errorf("expected ID 5, got %d", u.ID)
	}
	username := string(bytes.TrimRight(u.Username[:], "\x00"))
	if username != "user" {
		t.Errorf("expected username 'user', got %q", username)
	}

	// Not found
	u, err = table.FindUser(999)
	if err != nil {
		t.Fatal(err)
	}
	if u != nil {
		t.Errorf("expected nil for non-existent user, got %+v", u)
	}
}

func TestDeleteUser(t *testing.T) {
	table, cleanup := setupBenchTable(t, 10)
	defer cleanup()

	// Delete user 5
	if err := table.DeleteUser(5); err != nil {
		t.Fatal(err)
	}

	// FindUser should return nil for deleted user
	u, err := table.FindUser(5)
	if err != nil {
		t.Fatal(err)
	}
	if u != nil {
		t.Errorf("expected nil for deleted user, got %+v", u)
	}

	// SelectAll should skip deleted user (9 users remain)
	users, err := table.SelectAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(users) != 9 {
		t.Errorf("expected 9 users after delete, got %d", len(users))
	}
	for _, u := range users {
		if u.ID == 5 {
			t.Error("deleted user 5 should not appear in SelectAll")
		}
	}

	// NumRows should be unchanged (tombstone)
	if table.NumRows != 10 {
		t.Errorf("expected NumRows=10 (tombstone), got %d", table.NumRows)
	}
}

func TestConcurrentInsertFind(t *testing.T) {
	table, cleanup := setupBenchTable(t, 0)
	defer cleanup()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			baseID := uint32(id*1000 + 1)
			for j := uint32(0); j < 10; j++ {
				var username [32]byte
				copy(username[:], []byte("concurrent"))
				_ = table.InsertRow(User{ID: baseID + j, Username: username})
				_, _ = table.FindUser(baseID + j)
			}
		}(i)
	}
	wg.Wait()

	users, err := table.SelectAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(users) == 0 {
		t.Error("expected some users after concurrent insert")
	}
}
