package main

import (
	"log"
	"net"
	"os"

	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "github.com/AndrewK4758/shared_protos"
	"human_validation_service/internal/server"
)

func main() {
	// Attempt to load .env, ignore error if it doesn't exist
	_ = godotenv.Load()

	port := os.Getenv("PORT")
	if port == "" {
		port = "50054" // Using 50054 (AI=50051, Orchestrator=50052, HumanValidation=50054)
	}

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to listen on port %s: %v", port, err)
	}

	orchestratorAddr := os.Getenv("ORCHESTRATOR_ADDRESS")
	if orchestratorAddr == "" {
		orchestratorAddr = "localhost:50052"
	}

	grpcServer := grpc.NewServer()

	// Register the Human Validation service and inject orchestrator connection address
	validationServer := server.NewHumanValidationServer(orchestratorAddr)
	pb.RegisterHumanValidationServiceServer(grpcServer, validationServer)

	// Register reflection service on gRPC server so clients can query it
	reflection.Register(grpcServer)

	log.Printf("Human Validation Service starting on port %s...", port)
	log.Printf("Configured Orchestrator Address: %s", orchestratorAddr)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve gRPC server: %v", err)
	}
}
