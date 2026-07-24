package main

import (
	"context"
	"net"

	"github.com/AndrewK4758/shared_utils/envutil"

	"github.com/AndrewK4758/shared_utils/logger"
	"github.com/AndrewK4758/shared_utils/natsutil"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"human_validation_service/internal/server"

	pb "github.com/AndrewK4758/shared_protos"
)

func main() {
	// Attempt to load .env, ignore error if it doesn't exist
	envutil.LoadEnv(".env", "human_validation_service/.env", "../.env", "../../.env")
	logger.InitLogger()

	natsURL := envutil.GetStringOrDefault("NATS_URL", "")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}
	nc, err := natsutil.ConnectWithRetries(natsURL, "HumanValidation")
	if err != nil {
		logger.Fatal("HumanValidation", "Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		logger.Fatal("HumanValidation", "Failed to init JetStream: %v", err)
	}

	natsServer := server.NewNatsServer(nc, js)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := natsServer.Start(ctx); err != nil {
			logger.Fatal("HumanValidation", "NATS Server failed: %v", err)
		}
	}()

	port := envutil.GetStringOrDefault("PORT", "")
	if port == "" {
		port = "50056" // Using 50056
	}

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		logger.Fatal("HumanValidation", "Failed to listen on port %s: %v", port, err)
	}

	orchestratorAddr := envutil.GetStringOrDefault("ORCHESTRATOR_ADDRESS", "")
	if orchestratorAddr == "" {
		orchestratorAddr = "localhost:50052"
	}

	grpcServer := grpc.NewServer()

	// Register the Human Validation service and inject orchestrator connection address
	validationServer := server.NewHumanValidationServer(orchestratorAddr)
	pb.RegisterHumanValidationServiceServer(grpcServer, validationServer)

	// Register reflection service on gRPC server so clients can query it
	reflection.Register(grpcServer)

	logger.Info("HumanValidation", "Human Validation Service starting on port %s...", port)
	logger.Info("HumanValidation", "Configured Orchestrator Address: %s", orchestratorAddr)
	if err := grpcServer.Serve(listener); err != nil {
		logger.Fatal("HumanValidation", "Failed to serve gRPC server: %v", err)
	}
}
