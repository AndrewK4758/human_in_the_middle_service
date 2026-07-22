package main

import (
	"context"
	"log"
	"net"
	"os"

	"github.com/AndrewK4758/shared_utils/logger"

	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"human_validation_service/internal/server"
	"human_validation_service/internal/worker"

	pb "github.com/AndrewK4758/shared_protos"
)

func main() {
	logger.InitLogger()
	// Attempt to load .env, ignore error if it doesn't exist
	_ = godotenv.Load()

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("Failed to init JetStream: %v", err)
	}

	natsServer := server.NewNatsServer(nc, js)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := natsServer.Start(ctx); err != nil {
			log.Fatalf("NATS Server failed: %v", err)
		}
	}()

	port := os.Getenv("PORT")
	if port == "" {
		port = "50056" // Using 50056
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

	// Start the NATS worker
	worker, err := worker.NewWorker()
	if err != nil {
		log.Fatalf("Failed to create worker: %v", err)
	}
	defer worker.Close()

	go func() {
		log.Printf("Starting JetStream Human Validation Worker...")
		if err := worker.Start(context.Background()); err != nil {
			log.Fatalf("Worker failed: %v", err)
		}
	}()

	log.Printf("Human Validation Service starting on port %s...", port)
	log.Printf("Configured Orchestrator Address: %s", orchestratorAddr)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve gRPC server: %v", err)
	}
}
