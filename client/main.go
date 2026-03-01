package main

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sql-clone/pb" // Replace 'sql-clone' with the module name in your go.mod
)

func main() {
	// 1. Connect to the server running in your other terminal
	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewDatabaseServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// 2. Test an INSERT via Network
	log.Println("Sending Insert request...")
	r, err := c.Insert(ctx, &pb.InsertRequest{
		User: &pb.User{Id: 999, Username: "Vivek_Networked"},
	})
	if err != nil {
		log.Fatalf("could not insert: %v", err)
	}
	log.Printf("Server Response: %s", r.GetMessage())

	// 3. Test a FIND via Network
	log.Println("Sending Find request...")
	user, err := c.Find(ctx, &pb.FindRequest{Id: 999})
	if err != nil {
		log.Fatalf("could not find: %v", err)
	}
	log.Printf("Found User: ID=%d, Name=%s", user.Id, user.Username)
}