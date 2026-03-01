package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

// This starts a local HTTP server on port 8080
func startWebServer(t *Table) {
	// 1. Serve the Frontend HTML
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})

	// 2. API to Get a User
	http.HandleFunc("/api/find", func(w http.ResponseWriter, r *http.Request) {
		idStr := r.URL.Query().Get("id")
		id, _ := strconv.Atoi(idStr)

		user, err := t.FindUser(uint32(id))
		if err != nil || user == nil {
			http.Error(w, "User not found", 404)
			return
		}

		// Convert binary name to clean string for the UI
		cleanName := string(user.Username[:])
		json.NewEncoder(w).Encode(map[string]interface{}{
			"id":   user.ID,
			"name": cleanName,
		})
	})

	fmt.Println("🌍 Frontend available at http://localhost:8080")
	http.ListenAndServe(":8080", nil)
}