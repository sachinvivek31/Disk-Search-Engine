package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

func database(t *Table) {
	pager, err := NewPager("test.db")
	if err != nil {
		fmt.Printf("Error creating pager: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		if err := pager.file.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "Error closing database file: %v\n", err)
		}
	}()

	table, err := NewTable(pager)
	if err != nil {
		fmt.Printf("Error creating table: %v\n", err)
		os.Exit(1)
	}

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("SQL Clone REPL. Commands: exit | select | insert [id] [name] | find [id] | delete [id] | stress")

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
			}
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, " ", 3)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "exit":
			return
		case "select":
			users, err := table.SelectAll()
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}
			// Print in a nice table format
			fmt.Println(" id | username")
			fmt.Println("----+----------------------------------")
			for _, u := range users {
				username := string(bytes.TrimRight(u.Username[:], "\x00"))
				fmt.Printf("%3d | %s\n", u.ID, username)
			}
			fmt.Printf("(%d row(s))\n", len(users))
		case "insert":
			if len(parts) < 3 {
				fmt.Println("Usage: insert [id] [name]")
				continue
			}
			id64, err := strconv.ParseUint(parts[1], 10, 32)
			if err != nil {
				fmt.Printf("Invalid id: %v\n", err)
				continue
			}
			name := parts[2]
			var username [32]byte
			copy(username[:], []byte(name))
			u := User{
				ID:       uint32(id64),
				Username: username,
			}
			if err := table.InsertRow(u); err != nil {
				fmt.Printf("Error inserting row: %v\n", err)
				continue
			}
			fmt.Println("Inserted 1 row.")
		case "find":
			if len(parts) < 2 {
				fmt.Println("Usage: find [id]")
				continue
			}
			id64, err := strconv.ParseUint(parts[1], 10, 32)
			if err != nil {
				fmt.Printf("Invalid id: %v\n", err)
				continue
			}
			u, err := table.FindUser(uint32(id64))
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}
			if u == nil {
				fmt.Println("User not found.")
			} else {
				username := string(bytes.TrimRight(u.Username[:], "\x00"))
				fmt.Println(username)
			}
		case "delete":
			if len(parts) < 2 {
				fmt.Println("Usage: delete [id]")
				continue
			}
			id64, err := strconv.ParseUint(parts[1], 10, 32)
			if err != nil {
				fmt.Printf("Invalid id: %v\n", err)
				continue
			}
			if err := table.DeleteUser(uint32(id64)); err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}
			fmt.Println("Deleted.")
		case "stress":
			var wg sync.WaitGroup
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					baseID := uint32(id*1000 + 1)
					for j := uint32(0); j < 10; j++ {
						var username [32]byte
						copy(username[:], []byte("stress"))
						_ = table.InsertRow(User{ID: baseID + j, Username: username})
						_, _ = table.FindUser(baseID + j)
					}
				}(i)
			}
			wg.Wait()
			fmt.Println("Stress test complete: 10 goroutines × 10 inserts + 10 finds each.")
		default:
			fmt.Printf("Unknown command %q. Use: exit | select | insert [id] [name] | find [id] | delete [id] | stress\n", cmd)
		}
	}
}
