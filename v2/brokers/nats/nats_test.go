package nats

import (
	"context"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v2/config"
	"github.com/RichardKnop/machinery/v2/tasks"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	cnf := &config.Config{
		Broker:        "nats://localhost:4222",
		DefaultQueue:  "machinery_tasks",
		ResultBackend: "redis://localhost:6379",
		NATS: &config.NATSConfig{
			URL:           "nats://localhost:4222",
			MaxReconnects: 5,
			ReconnectWait: 2 * time.Second,
			Timeout:       5 * time.Second,
			SubjectPrefix: "machinery",
		},
	}

	broker := New(cnf)
	assert.NotNil(t, broker)
	assert.Equal(t, "nats://localhost:4222", broker.(*Broker).url)
	assert.Equal(t, 5, broker.(*Broker).maxReconnects)
	assert.Equal(t, 2*time.Second, broker.(*Broker).reconnectWait)
	assert.Equal(t, 5*time.Second, broker.(*Broker).timeout)
	assert.Equal(t, "machinery", broker.(*Broker).subjectPrefix)
}

func TestNewWithDefaults(t *testing.T) {
	cnf := &config.Config{
		Broker:        "nats://localhost:4222",
		DefaultQueue:  "machinery_tasks",
		ResultBackend: "redis://localhost:6379",
	}

	broker := New(cnf)
	assert.NotNil(t, broker)
	assert.Equal(t, "nats://localhost:4222", broker.(*Broker).url)
	assert.Equal(t, defaultMaxReconnects, broker.(*Broker).maxReconnects)
	assert.Equal(t, defaultReconnectWait, broker.(*Broker).reconnectWait)
	assert.Equal(t, defaultTimeout, broker.(*Broker).timeout)
	assert.Equal(t, defaultSubjectPrefix, broker.(*Broker).subjectPrefix)
}

func TestGetSubject(t *testing.T) {
	cnf := &config.Config{
		Broker:        "nats://localhost:4222",
		DefaultQueue:  "machinery_tasks",
		ResultBackend: "redis://localhost:6379",
		NATS: &config.NATSConfig{
			SubjectPrefix: "test",
		},
	}

	broker := New(cnf).(*Broker)

	subject := broker.getSubject("test_queue")
	assert.Equal(t, "test.test_queue", subject)
}

func TestGetSubjectWithEmptyPrefix(t *testing.T) {
	cnf := &config.Config{
		Broker:        "nats://localhost:4222",
		DefaultQueue:  "machinery_tasks",
		ResultBackend: "redis://localhost:6379",
		NATS: &config.NATSConfig{
			SubjectPrefix: "",
		},
	}

	broker := New(cnf).(*Broker)

	subject := broker.getSubject("test_queue")
	assert.Equal(t, "machinery.test_queue", subject)
}

func TestAdjustRoutingKey(t *testing.T) {
	cnf := &config.Config{
		Broker:        "nats://localhost:4222",
		DefaultQueue:  "machinery_tasks",
		ResultBackend: "redis://localhost:6379",
	}

	broker := New(cnf)

	signature := &tasks.Signature{
		Name: "test_task",
		Args: []tasks.Arg{},
	}

	// 测试空路由键
	broker.AdjustRoutingKey(signature)
	assert.Equal(t, "machinery_tasks", signature.RoutingKey)

	// 测试已有路由键
	signature.RoutingKey = "custom_queue"
	broker.AdjustRoutingKey(signature)
	assert.Equal(t, "custom_queue", signature.RoutingKey)
}

func TestConnect(t *testing.T) {
	// 这个测试需要运行中的 NATS 服务器
	// 在 CI 环境中可能会跳过
	if testing.Short() {
		t.Skip("跳过需要 NATS 服务器的测试")
	}

	cnf := &config.Config{
		Broker:        "nats://localhost:4222",
		DefaultQueue:  "machinery_tasks",
		ResultBackend: "redis://localhost:6379",
		NATS: &config.NATSConfig{
			URL:           "nats://localhost:4222",
			MaxReconnects: 1,
			ReconnectWait: 1 * time.Second,
			Timeout:       2 * time.Second,
		},
	}

	broker := New(cnf).(*Broker)

	err := broker.connect()
	if err != nil {
		t.Skipf("无法连接到 NATS 服务器: %v", err)
	}

	assert.NotNil(t, broker.conn)
	assert.True(t, broker.conn.IsConnected())

	// 清理
	broker.conn.Close()
}

func TestConnectWithInvalidURL(t *testing.T) {
	cnf := &config.Config{
		Broker:        "nats://invalid:4222",
		DefaultQueue:  "machinery_tasks",
		ResultBackend: "redis://localhost:6379",
		NATS: &config.NATSConfig{
			URL:           "nats://invalid:4222",
			MaxReconnects: 0,
			ReconnectWait: 1 * time.Second,
			Timeout:       1 * time.Second,
		},
	}

	broker := New(cnf).(*Broker)

	err := broker.connect()
	assert.Error(t, err)
	assert.Nil(t, broker.conn)
}

func TestPublishWithoutConnection(t *testing.T) {
	cnf := &config.Config{
		Broker:        "nats://localhost:4222",
		DefaultQueue:  "machinery_tasks",
		ResultBackend: "redis://localhost:6379",
		NATS: &config.NATSConfig{
			URL:           "nats://invalid:4222",
			MaxReconnects: 0,
			ReconnectWait: 1 * time.Second,
			Timeout:       1 * time.Second,
		},
	}

	broker := New(cnf)

	signature := &tasks.Signature{
		Name: "test_task",
		Args: []tasks.Arg{
			{Type: "string", Value: "test"},
		},
	}

	ctx := context.Background()
	err := broker.Publish(ctx, signature)
	assert.Error(t, err)
}

func TestDelay(t *testing.T) {
	cnf := &config.Config{
		Broker:        "nats://localhost:4222",
		DefaultQueue:  "machinery_tasks",
		ResultBackend: "redis://localhost:6379",
	}

	broker := New(cnf).(*Broker)

	signature := &tasks.Signature{
		Name: "test_task",
		Args: []tasks.Arg{
			{Type: "string", Value: "test"},
		},
	}

	// 测试无效延迟
	err := broker.delay(signature, 0)
	assert.Error(t, err)

	// 测试负延迟
	err = broker.delay(signature, -time.Second)
	assert.Error(t, err)
}

func TestConsumeOne(t *testing.T) {
	cnf := &config.Config{
		Broker:        "nats://localhost:4222",
		DefaultQueue:  "machinery_tasks",
		ResultBackend: "redis://localhost:6379",
	}

	broker := New(cnf).(*Broker)

	// 注册一个测试任务
	broker.SetRegisteredTaskNames([]string{"test_task"})

	// 创建模拟的 TaskProcessor
	processor := &mockTaskProcessor{}

	// 测试空消息
	err := broker.consumeOne(nil, processor)
	assert.Error(t, err)

	// 测试无效 JSON
	// 这里需要创建一个模拟的 nats.Msg，但由于 nats.Msg 是私有结构，
	// 我们无法直接创建，所以这个测试在实际实现中可能需要调整
}

// mockTaskProcessor 用于测试的模拟 TaskProcessor
type mockTaskProcessor struct{}

func (m *mockTaskProcessor) Process(signature *tasks.Signature) error {
	return nil
}

func (m *mockTaskProcessor) CustomQueue() string {
	return ""
}

func (m *mockTaskProcessor) PreConsumeHandler() bool {
	return true
}

func TestGetPendingTasks(t *testing.T) {
	cnf := &config.Config{
		Broker:        "nats://localhost:4222",
		DefaultQueue:  "machinery_tasks",
		ResultBackend: "redis://localhost:6379",
	}

	broker := New(cnf)

	tasks, err := broker.GetPendingTasks("test_queue")
	assert.NoError(t, err)
	assert.Empty(t, tasks)
}

func TestGetDelayedTasks(t *testing.T) {
	cnf := &config.Config{
		Broker:        "nats://localhost:4222",
		DefaultQueue:  "machinery_tasks",
		ResultBackend: "redis://localhost:6379",
	}

	broker := New(cnf)

	tasks, err := broker.GetDelayedTasks()
	assert.NoError(t, err)
	assert.Empty(t, tasks)
}

// 集成测试（需要运行中的 NATS 服务器）
func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过集成测试")
	}

	cnf := &config.Config{
		Broker:        "nats://localhost:4222",
		DefaultQueue:  "machinery_tasks",
		ResultBackend: "redis://localhost:6379",
		NATS: &config.NATSConfig{
			URL:           "nats://localhost:4222",
			MaxReconnects: 5,
			ReconnectWait: 2 * time.Second,
			Timeout:       5 * time.Second,
		},
	}

	broker := New(cnf)

	// 测试连接
	err := broker.(*Broker).connect()
	if err != nil {
		t.Skipf("无法连接到 NATS 服务器: %v", err)
	}
	defer broker.(*Broker).conn.Close()

	// 测试发布
	signature := &tasks.Signature{
		Name: "test_task",
		Args: []tasks.Arg{
			{Type: "string", Value: "test"},
		},
	}

	ctx := context.Background()
	err = broker.Publish(ctx, signature)
	assert.NoError(t, err)

	// 测试延迟发布
	eta := time.Now().UTC().Add(time.Second)
	signature.ETA = &eta
	err = broker.Publish(ctx, signature)
	assert.NoError(t, err)
}
