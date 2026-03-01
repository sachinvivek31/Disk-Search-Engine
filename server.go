package main

import (
	"context"
	"bytes"
	"log"
	"net"

	"sql-clone/pb" // Replace with your actual module name from go.mod

	"google.golang.org/grpc"
)

type dbServer struct {
	pb.UnimplementedDatabaseServiceServer
	table *Table
}

// Insert handles network requests to add a user
func (s *dbServer) Insert(ctx context.Context, req *pb.InsertRequest) (*pb.Response, error) {
    // 1. Create an empty 32-byte array (initialized with null bytes)
    var fixedUsername [32]byte
    
    // 2. Copy the string content into the array
    // This safely handles strings longer than 32 chars by truncating them,
    // and strings shorter than 32 chars by leaving the rest as null bytes (\x00).
    copy(fixedUsername[:], req.User.Username)

    // 3. Create the User struct using the fixed-size array
    u := User{
        ID:        req.User.Id,
        Username:  fixedUsername, // Now the types match!
        IsDeleted: false,
    }

    err := s.table.InsertRow(u)
    if err != nil {
        return nil, err
    }
    return &pb.Response{Message: "Successfully inserted via gRPC"}, nil
}

// Find handles O(log N) lookups over the network
func (s *dbServer) Find(ctx context.Context, req *pb.FindRequest) (*pb.User, error) {
    // 1. Perform the O(log N) lookup in your B+ Tree
    user, err := s.table.FindUser(req.Id)
    if err != nil || user == nil {
        return nil, err
    }

    // 2. Convert [32]byte to a Go string
    // We use bytes.Trim to remove the trailing null characters (\x00) 
    // that fill the rest of the 32-byte buffer on disk.
    cleanName := string(bytes.Trim(user.Username[:], "\x00"))

    return &pb.User{
        Id:       user.ID,
        Username: cleanName,
        IsDeleted: user.IsDeleted,
    }, nil
}

// startGRPCServer encapsulates the network networking logic
func startGRPCServer(t *Table) {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen on port 50051: %v", err)
	}

	s := grpc.NewServer()
	
	// Dependency Injection: Passing the table to the server struct
	pb.RegisterDatabaseServiceServer(s, &dbServer{table: t})

	log.Println("🚀 gRPC Server running on :50051...")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("gRPC Server failed: %v", err)
	}
}

func main() {
	// 1. Core Engine Initialization
	pager, err := NewPager("test.db")
	if err != nil {
		log.Fatalf("Fatal: Could not initialize pager: %v", err)
	}
	table, err := NewTable(pager)
	if err != nil {
		log.Fatalf("Fatal: Could not initialize table: %v", err)
	}

	// 2. Start gRPC Network Layer (Background)
	go startGRPCServer(table)

	// 3. Start Web Dashboard Gateway (Background)
	go startWebServer(table)

	// 4. Start Local CLI REPL (Foreground - Keeps the app alive)
	database(table)
}