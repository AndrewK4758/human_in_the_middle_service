package server

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/AndrewK4758/shared_protos"
)

type mockJetStream struct {
	jetstream.JetStream
	mu            sync.Mutex
	publishedData map[string][]byte
	publishErr    error
}

func newMockJetStream() *mockJetStream {
	return &mockJetStream{
		publishedData: make(map[string][]byte),
	}
}

func (m *mockJetStream) Publish(ctx context.Context, subject string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.publishErr != nil {
		return nil, m.publishErr
	}
	m.publishedData[subject] = data
	return &jetstream.PubAck{Stream: "ORCHESTRATOR_RESPONSES"}, nil
}

type mockMsg struct {
	data     []byte
	subject  string
	headers  nats.Header
	acked    bool
	nacked   bool
	nakDelay time.Duration
}

func (m *mockMsg) Data() []byte         { return m.data }
func (m *mockMsg) Subject() string      { return m.subject }
func (m *mockMsg) Headers() nats.Header { return m.headers }
func (m *mockMsg) Reply() string        { return "" }
func (m *mockMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{NumDelivered: 1}, nil
}
func (m *mockMsg) Ack() error {
	m.acked = true
	return nil
}
func (m *mockMsg) DoubleAck(ctx context.Context) error {
	m.acked = true
	return nil
}
func (m *mockMsg) Nak() error {
	m.nacked = true
	return nil
}
func (m *mockMsg) NakWithDelay(delay time.Duration) error {
	m.nacked = true
	m.nakDelay = delay
	return nil
}
func (m *mockMsg) InProgress() error { return nil }
func (m *mockMsg) Term() error {
	m.nacked = true
	return nil
}
func (m *mockMsg) TermWithReason(reason string) error {
	m.nacked = true
	return nil
}

func init() {
	os.Setenv("WORKERS_STREAM", "WORKERS")
}

func TestNatsServer_ProcessMessage_SuccessAndTaskCreation(t *testing.T) {
	valServer := newMockHumanValidationServer()
	mockJS := newMockJetStream()

	natsServer := &NatsServer{
		nc:               nil,
		js:               mockJS,
		validationServer: valServer,
	}

	var expectedSchemaMap map[string]any
	_ = json.Unmarshal([]byte(`{"type":"object"}`), &expectedSchemaMap)

	payloadMap := map[string]any{
		"node_config": map[string]any{
			"node_name":              "ClassificationNode",
			"expected_schema_struct": expectedSchemaMap,
			"system_instructions":    "Classify incoming document",
		},
		"human_validation": map[string]any{
			"required":        true,
			"bypass_fallback": false,
		},
		"global_state": map[string]any{
			"functional": map[string]any{
				"step_id": "step_classify_1",
			},
		},
		"metadata": map[string]any{
			"env": "testing",
		},
	}
	payloadStruct, err := structpb.NewStruct(payloadMap)
	if err != nil {
		t.Fatalf("Failed to create payload struct: %v", err)
	}

	req := &pb.ExecuteTaskRequest{
		Identity: &pb.InfrastructureIdentity{
			TenantId: "tenant_test",
			AppId:    "app_test",
			JobId:    "job_test_123",
		},
		TaskSubject: "worker.human.validation.step1",
		StepId:      "step_classify_1",
		Payload:     payloadStruct,
	}

	reqBytes, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("Failed to marshal request: %v", err)
	}

	msg := &mockMsg{
		data:    reqBytes,
		subject: "worker.human.validation.step1",
	}

	ctx := context.Background()
	natsServer.processMessage(ctx, msg)

	if !msg.acked {
		t.Errorf("Expected message to be acknowledged")
	}

	valServer.mu.RLock()
	defer valServer.mu.RUnlock()

	if len(valServer.tasks) != 1 {
		t.Fatalf("Expected 1 pending task in HumanValidationServer, got %d", len(valServer.tasks))
	}

	var task *pb.PendingTask
	for _, v := range valServer.tasks {
		task = v
		break
	}

	if task.GetIdentity() == nil || task.GetIdentity().GetJobId() != "job_test_123" {
		t.Errorf("Expected JobId 'job_test_123', got %v", task.GetIdentity())
	}
	if task.GetStepId() != "step_classify_1" {
		t.Errorf("Expected StepId 'step_classify_1', got %s", task.GetStepId())
	}
	if task.GetStepName() != "ClassificationNode" {
		t.Errorf("Expected StepName 'ClassificationNode', got %s", task.GetStepName())
	}
	if task.GetPromptInstructions() != "Classify incoming document" {
		t.Errorf("Expected PromptInstructions 'Classify incoming document', got %s", task.GetPromptInstructions())
	}
	if task.GetExpectedSchema() == nil || len(task.GetExpectedSchema().Fields) == 0 {
		t.Errorf("Expected ExpectedJsonSchema '{\"type\":\"object\"}', got %s", task.GetExpectedSchema())
	}
	if task.GetStatus() != TaskStatusPending {
		t.Errorf("Expected status %s, got %s", TaskStatusPending, task.GetStatus())
	}
}

func TestNatsServer_ProcessMessage_PerformActionResponseWithErrorContract(t *testing.T) {
	valServer := newMockHumanValidationServer()
	mockJS := newMockJetStream()

	natsServer := &NatsServer{
		nc:               nil,
		js:               mockJS,
		validationServer: valServer,
	}

	payloadStruct, err := structpb.NewStruct(map[string]any{
		"node_config": map[string]any{
			"node_name": "HumanValidationStep",
		},
		"global_state": map[string]any{
			"functional": map[string]any{
				"step_id": "step_val_2",
			},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create payload struct: %v", err)
	}

	req := &pb.ExecuteTaskRequest{
		Identity: &pb.InfrastructureIdentity{
			TenantId: "t1",
			AppId:    "a1",
			JobId:    "job_test_response",
		},
		TaskSubject: "worker.human.validation",
		StepId:      "step_val_2",
		Payload:     payloadStruct,
	}

	reqBytes, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("Failed to marshal request: %v", err)
	}

	replySubject := "orchestrator.response.reply.123"
	headers := nats.Header{}
	headers.Set("Reply-Subject", replySubject)

	msg := &mockMsg{
		data:    reqBytes,
		subject: "worker.human.validation",
		headers: headers,
	}

	ctx := context.Background()
	natsServer.processMessage(ctx, msg)

	if !msg.acked {
		t.Errorf("Expected message to be acknowledged")
	}

	mockJS.mu.Lock()
	replyData, exists := mockJS.publishedData[replySubject]
	mockJS.mu.Unlock()

	if !exists {
		t.Fatalf("Expected reply to be published to %s", replySubject)
	}

	var resp pb.PerformActionResponse
	if err := proto.Unmarshal(replyData, &resp); err != nil {
		t.Fatalf("Failed to unmarshal PerformActionResponse: %v", err)
	}

	if resp.GetSuccess() {
		t.Errorf("Expected response Success to be false")
	}
	if resp.GetIsTransientError() {
		t.Errorf("Expected IsTransientError to be false")
	}

	errContract := resp.GetErrorContract()
	if errContract == nil {
		t.Fatalf("Expected ErrorContract to be non-nil")
	}

	if errContract.GetCode() != "HUMAN_VALIDATION_REQUIRED" {
		t.Errorf("Expected Code 'HUMAN_VALIDATION_REQUIRED', got %s", errContract.GetCode())
	}
	if errContract.GetSource() != "human_validation_service" {
		t.Errorf("Expected Source 'human_validation_service', got %s", errContract.GetSource())
	}

	payloadStruct = errContract.GetClientPayload()
	if payloadStruct == nil {
		t.Fatalf("Expected ClientPayload to be non-nil")
	}

	detailVal, ok := payloadStruct.Fields["detail"]
	if !ok {
		t.Fatalf("Expected 'detail' field in ClientPayload")
	}
	detailStr := detailVal.GetStringValue()
	if detailStr == "" {
		t.Errorf("Expected non-empty detail string in ClientPayload")
	}
}

func TestNatsServer_ProcessMessage_InvalidProtobufBytes(t *testing.T) {
	valServer := newMockHumanValidationServer()
	mockJS := newMockJetStream()

	natsServer := &NatsServer{
		nc:               nil,
		js:               mockJS,
		validationServer: valServer,
	}

	msg := &mockMsg{
		data:    []byte("invalid proto payload data"),
		subject: "worker.human.validation",
	}

	ctx := context.Background()
	natsServer.processMessage(ctx, msg)

	if !msg.nacked {
		t.Errorf("Expected message to be NACKed when given invalid protobuf bytes")
	}
	if msg.acked {
		t.Errorf("Message should not be ACKed on invalid protobuf bytes")
	}
}

func TestNatsServer_ProcessMessage_GraphIdAndInitialNodeIdExtraction(t *testing.T) {
	valServer := newMockHumanValidationServer()
	mockJS := newMockJetStream()

	natsServer := &NatsServer{
		nc:               nil,
		js:               mockJS,
		validationServer: valServer,
	}

	payloadStruct, err := structpb.NewStruct(map[string]any{
		"node_config": map[string]any{
			"node_name": "ExtractionNode",
		},
		"global_state": map[string]any{
			"functional": map[string]any{
				"step_id": "step_extract_1",
			},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create payload struct: %v", err)
	}

	req := &pb.ExecuteTaskRequest{
		Identity: &pb.InfrastructureIdentity{
			TenantId: "tenant_graph_test",
			AppId:    "app_graph_test",
			JobId:    "job_graph_test_789",
		},
		TaskSubject: "worker.human.validation.extract",
		StepId:      "step_extract_1",
		Payload:     payloadStruct,
	}

	reqBytes, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("Failed to marshal request: %v", err)
	}

	msg := &mockMsg{
		data:    reqBytes,
		subject: "worker.human.validation.extract",
	}

	ctx := context.Background()
	natsServer.processMessage(ctx, msg)

	if !msg.acked {
		t.Errorf("Expected message with GraphId and InitialNodeId to be acknowledged")
	}

	valServer.mu.RLock()
	defer valServer.mu.RUnlock()

	if len(valServer.tasks) != 1 {
		t.Fatalf("Expected 1 pending task, got %d", len(valServer.tasks))
	}
}

func TestNatsServer_ProcessMessage_MissingIdentityFailFast(t *testing.T) {
	valServer := newMockHumanValidationServer()
	mockJS := newMockJetStream()

	natsServer := &NatsServer{
		nc:               nil,
		js:               mockJS,
		validationServer: valServer,
	}

	payloadStruct, _ := structpb.NewStruct(map[string]any{
		"node_config": map[string]any{
			"node_name": "NoIdentityNode",
		},
		"global_state": map[string]any{
			"functional": map[string]any{
				"step_id": "step_no_identity",
			},
		},
	})
	req := &pb.ExecuteTaskRequest{
		TaskSubject: "worker.human.validation",
		StepId:      "step_no_identity",
		Payload:     payloadStruct,
	}

	reqBytes, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("Failed to marshal request: %v", err)
	}

	msg := &mockMsg{
		data:    reqBytes,
		subject: "worker.human.validation",
	}

	ctx := context.Background()
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Expected panic for missing identity")
			}
		}()
		natsServer.processMessage(ctx, msg)
	}()
}

type mockHumanValidationServer struct {
	mu    sync.RWMutex
	tasks map[string]*pb.PendingTask
}

func newMockHumanValidationServer() *mockHumanValidationServer {
	return &mockHumanValidationServer{tasks: make(map[string]*pb.PendingTask)}
}

func (m *mockHumanValidationServer) CreateValidationTask(ctx context.Context, req *pb.CreateTaskRequest) (*pb.CreateTaskResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	taskID := "mock-task-123"
	m.tasks[taskID] = &pb.PendingTask{
		TaskId:             taskID,
		Identity:           req.GetIdentity(),
		StepName:           req.GetStepName(),
		PromptInstructions: req.GetPromptInstructions(),
		ExpectedSchema:     req.GetExpectedSchema(),
		Status:             TaskStatusPending,
		StepId:             req.GetStepId(),
	}
	return &pb.CreateTaskResponse{TaskId: taskID, Status: TaskStatusCreated}, nil
}

func TestHandleMessageWithRecovery_Panic_NakWithDelay(t *testing.T) {
	server := &NatsServer{}
	msg := &mockMsg{
		subject: "worker.doc.test",
	}
	sem := make(chan struct{}, 1)

	// Empty message payload triggers a validation panic in processMessage.
	server.HandleMessageWithRecovery(context.Background(), msg, sem)

	if !msg.nacked {
		t.Errorf("Expected NakWithDelay to be called on panic")
	}
}
