package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

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

func (m *mockMsg) Data() []byte { return m.data }
func (m *mockMsg) Subject() string { return m.subject }
func (m *mockMsg) Headers() nats.Header { return m.headers }
func (m *mockMsg) Reply() string { return "" }
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
func (m *mockMsg) Term() error { return nil }
func (m *mockMsg) TermWithReason(reason string) error { return nil }

func TestNatsServer_ProcessMessage_SuccessAndTaskCreation(t *testing.T) {
	valServer := NewHumanValidationServer("localhost:11112")
	mockJS := newMockJetStream()

	natsServer := &NatsServer{
		nc:               nil,
		js:               mockJS,
		validationServer: valServer,
	}

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity: &pb.InfrastructureIdentity{
			TenantId: "tenant_test",
			AppId:    "app_test",
			JobId:    "job_test_123",
		},
		GraphId:       "graph_test_123",
		InitialNodeId: "node_init_123",
		StepId:        "step_classify_1",
		NodeConfig: &pb.NodeConfiguration{
			NodeName:           "ClassificationNode",
			SystemInstructions: "Classify incoming document",
			ExpectedSchemaStruct: unmarshalStruct(`{"type":"object"}`),
		},
		GlobalState: &pb.GlobalState{},
		HumanValidation: &pb.HumanValidationContract{
			Required:       true,
			BypassFallback: false,
		},
		Metadata: map[string]string{
			"env": "testing",
		},
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
	valServer := NewHumanValidationServer("localhost:11112")
	mockJS := newMockJetStream()

	natsServer := &NatsServer{
		nc:               nil,
		js:               mockJS,
		validationServer: valServer,
	}

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity: &pb.InfrastructureIdentity{
			TenantId: "t1",
			AppId:    "a1",
			JobId:    "job_test_response",
		},
		GraphId:       "graph_test_123",
		InitialNodeId: "node_init_123",
		StepId:        "step_val_2",
		NodeConfig: &pb.NodeConfiguration{
			NodeName: "HumanValidationStep",
		},
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

	payloadStruct := errContract.GetClientPayload()
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
	valServer := NewHumanValidationServer("localhost:11112")
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
	valServer := NewHumanValidationServer("localhost:11112")
	mockJS := newMockJetStream()

	natsServer := &NatsServer{
		nc:               nil,
		js:               mockJS,
		validationServer: valServer,
	}

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity: &pb.InfrastructureIdentity{
			TenantId: "tenant_graph_test",
			AppId:    "app_graph_test",
			JobId:    "job_graph_test_789",
		},
		GraphId:       "graph_extract_001",
		InitialNodeId: "node_init_extract_002",
		StepId:        "step_extract_1",
		NodeConfig: &pb.NodeConfiguration{
			NodeName: "ExtractionNode",
		},
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
	valServer := NewHumanValidationServer("localhost:11112")
	mockJS := newMockJetStream()

	natsServer := &NatsServer{
		nc:               nil,
		js:               mockJS,
		validationServer: valServer,
	}

	// Request with missing identity
	req := &pb.ExecuteWorkflowNodeRequest{
		StepId: "step_no_identity",
		NodeConfig: &pb.NodeConfiguration{
			NodeName: "NoIdentityNode",
		},
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
	natsServer.processMessage(ctx, msg)

	if !msg.nacked {
		t.Errorf("Expected message with missing identity to be NACKed (fail-fast)")
	}
	if msg.acked {
		t.Errorf("Message with missing identity must NOT be ACKed")
	}
}


