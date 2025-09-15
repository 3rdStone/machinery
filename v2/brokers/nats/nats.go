package nats

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/RichardKnop/machinery/v2/brokers/errs"
	"github.com/RichardKnop/machinery/v2/brokers/iface"
	"github.com/RichardKnop/machinery/v2/common"
	"github.com/RichardKnop/machinery/v2/config"
	"github.com/RichardKnop/machinery/v2/log"
	"github.com/RichardKnop/machinery/v2/tasks"
)

const (
	// 默认主题前缀
	defaultSubjectPrefix = "machinery"
	// 默认最大重连次数
	defaultMaxReconnects = 5
	// 默认重连等待时间
	defaultReconnectWait = 2 * time.Second
	// 默认超时时间
	defaultTimeout = 5 * time.Second
)

// Broker 表示一个 NATS broker
type Broker struct {
	common.Broker
	conn               *nats.Conn
	js                 nats.JetStreamContext
	consumingWG        sync.WaitGroup // 等待组确保整个消费过程完成
	processingWG       sync.WaitGroup // 使用等待组确保任务处理完成
	delayedWG          sync.WaitGroup
	subjectPrefix      string
	subscriptions      map[string]*nats.Subscription
	subscriptionsMutex sync.RWMutex
	// 连接配置
	url           string
	maxReconnects int
	reconnectWait time.Duration
	timeout       time.Duration
	retryAttempts int
	retryDelay    time.Duration
}

// New 创建新的 Broker 实例
func New(cnf *config.Config) iface.Broker {
	b := &Broker{
		Broker:        common.NewBroker(cnf),
		subscriptions: make(map[string]*nats.Subscription),
	}

	// 设置默认配置
	if cnf.NATS != nil {
		b.url = cnf.NATS.URL
		b.maxReconnects = cnf.NATS.MaxReconnects
		b.reconnectWait = cnf.NATS.ReconnectWait
		b.timeout = cnf.NATS.Timeout
		b.retryAttempts = cnf.NATS.RetryAttempts
		b.retryDelay = cnf.NATS.RetryDelay
		b.subjectPrefix = cnf.NATS.SubjectPrefix
	} else {
		// 使用默认值
		b.url = "nats://localhost:4222"
		b.maxReconnects = defaultMaxReconnects
		b.reconnectWait = defaultReconnectWait
		b.timeout = defaultTimeout
		b.retryAttempts = 3
		b.retryDelay = 1 * time.Second
		b.subjectPrefix = defaultSubjectPrefix
	}

	if b.subjectPrefix == "" {
		b.subjectPrefix = defaultSubjectPrefix
	}

	return b
}

// StartConsuming 进入循环并等待传入的消息
func (b *Broker) StartConsuming(consumerTag string, concurrency int, taskProcessor iface.TaskProcessor) (bool, error) {
	b.consumingWG.Add(1)
	defer b.consumingWG.Done()

	if concurrency < 1 {
		concurrency = runtime.NumCPU() * 2
	}

	b.Broker.StartConsuming(consumerTag, concurrency, taskProcessor)

	// 连接到 NATS
	if err := b.connect(); err != nil {
		b.GetRetryFunc()(b.GetRetryStopChan())
		return b.GetRetry(), err
	}

	// 获取队列名称
	queueName := taskProcessor.CustomQueue()
	if queueName == "" {
		queueName = b.GetConfig().DefaultQueue
	}

	// 创建主题名称
	subject := b.getSubject(queueName)

	log.INFO.Print("[*] Waiting for messages. To exit press CTRL+C")

	// 启动消费
	if err := b.consume(subject, concurrency, taskProcessor); err != nil {
		return b.GetRetry(), err
	}

	// 等待任何正在处理的任务完成
	b.processingWG.Wait()

	return b.GetRetry(), nil
}

// StopConsuming 退出循环
func (b *Broker) StopConsuming() {
	b.Broker.StopConsuming()

	// 取消所有订阅
	b.subscriptionsMutex.Lock()
	for _, sub := range b.subscriptions {
		sub.Unsubscribe()
	}
	b.subscriptions = make(map[string]*nats.Subscription)
	b.subscriptionsMutex.Unlock()

	// 等待延迟任务 goroutine 停止
	b.delayedWG.Wait()
	// 等待消费完成
	b.consumingWG.Wait()
	// 等待当前正在处理的任务完成
	b.processingWG.Wait()

	// 关闭连接
	if b.conn != nil {
		b.conn.Close()
	}
}

// Publish 在默认队列上放置新消息
func (b *Broker) Publish(ctx context.Context, signature *tasks.Signature) error {
	// 调整路由键（这决定消息将发布到哪个队列）
	b.AdjustRoutingKey(signature)

	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	// 确保连接存在
	if b.conn == nil {
		if err := b.connect(); err != nil {
			return err
		}
	}

	// 检查 ETA 签名字段，如果设置了且在未来，延迟任务
	if signature.ETA != nil {
		now := time.Now().UTC()
		if signature.ETA.After(now) {
			return b.delay(signature, signature.ETA.Sub(now))
		}
	}

	// 获取主题名称
	subject := b.getSubject(signature.RoutingKey)

	// 发布消息
	return b.conn.Publish(subject, msg)
}

// GetPendingTasks 返回队列中等待的任务签名切片
func (b *Broker) GetPendingTasks(queue string) ([]*tasks.Signature, error) {
	// NATS 本身不提供队列检查功能，因为它是 fire-and-forget 模式
	// 如果需要查看待处理任务，建议使用 JetStream 或结果后端
	// 这里返回空切片，表示无法直接查询待处理任务
	log.INFO.Printf("NATS broker does not support GetPendingTasks - use JetStream or result backend for task monitoring")
	return []*tasks.Signature{}, nil
}

// GetDelayedTasks 返回已调度但尚未在队列中的任务签名切片
func (b *Broker) GetDelayedTasks() ([]*tasks.Signature, error) {
	// NATS 的延迟任务通过定时器实现，无法直接查询
	// 如果需要监控延迟任务，建议使用 JetStream 或结果后端
	log.INFO.Printf("NATS broker does not support GetDelayedTasks - use JetStream or result backend for delayed task monitoring")
	return []*tasks.Signature{}, nil
}

// AdjustRoutingKey 确保路由键正确
func (b *Broker) AdjustRoutingKey(s *tasks.Signature) {
	if s.RoutingKey != "" {
		return
	}
	s.RoutingKey = b.GetConfig().DefaultQueue
}

// connect 连接到 NATS 服务器
func (b *Broker) connect() error {
	if b.conn != nil && b.conn.IsConnected() {
		return nil
	}

	opts := []nats.Option{
		nats.MaxReconnects(b.maxReconnects),
		nats.ReconnectWait(b.reconnectWait),
		nats.Timeout(b.timeout),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.INFO.Printf("NATS disconnected: %v", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.INFO.Printf("NATS reconnected to %v", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			log.INFO.Printf("NATS connection closed")
		}),
	}

	// 尝试连接
	var err error
	for i := 0; i <= b.retryAttempts; i++ {
		b.conn, err = nats.Connect(b.url, opts...)
		if err == nil {
			break
		}
		if i < b.retryAttempts {
			log.INFO.Printf("NATS connection attempt %d failed: %v, retrying in %v", i+1, err, b.retryDelay)
			time.Sleep(b.retryDelay)
		}
	}

	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %v", err)
	}

	// 尝试创建 JetStream 上下文（用于延迟任务）
	b.js, _ = b.conn.JetStream()

	log.INFO.Printf("Connected to NATS at %s", b.conn.ConnectedUrl())
	return nil
}

// getSubject 根据队列名称生成主题名称
func (b *Broker) getSubject(queueName string) string {
	return fmt.Sprintf("%s.%s", b.subjectPrefix, queueName)
}

// consume 消费消息
func (b *Broker) consume(subject string, concurrency int, taskProcessor iface.TaskProcessor) error {
	// 创建订阅
	sub, err := b.conn.Subscribe(subject, func(msg *nats.Msg) {
		b.processingWG.Add(1)
		go func() {
			defer b.processingWG.Done()
			if err := b.consumeOne(msg, taskProcessor); err != nil {
				log.ERROR.Printf("Error processing message: %v", err)
			}
		}()
	})

	if err != nil {
		return fmt.Errorf("failed to subscribe to subject %s: %v", subject, err)
	}

	// 保存订阅以便后续取消
	b.subscriptionsMutex.Lock()
	b.subscriptions[subject] = sub
	b.subscriptionsMutex.Unlock()

	// 等待停止信号
	<-b.GetStopChan()

	return nil
}

// consumeOne 处理单个消息
func (b *Broker) consumeOne(msg *nats.Msg, taskProcessor iface.TaskProcessor) error {
	if len(msg.Data) == 0 {
		return fmt.Errorf("received empty message")
	}

	// 将消息体解组为签名结构
	signature := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewReader(msg.Data))
	decoder.UseNumber()
	if err := decoder.Decode(signature); err != nil {
		return errs.NewErrCouldNotUnmarshalTaskSignature(msg.Data, err)
	}

	// 如果任务未注册，我们重新排队
	// 可能有不同的工作器处理特定任务
	if !b.IsTaskRegistered(signature.Name) {
		if signature.IgnoreWhenTaskNotRegistered {
			return nil
		}
		log.INFO.Printf("Task not registered with this worker. Requeuing message: %s", msg.Data)
		// 重新发布消息
		return b.conn.Publish(msg.Subject, msg.Data)
	}

	log.DEBUG.Printf("Received new message: %s", msg.Data)

	return taskProcessor.Process(signature)
}

// delay 延迟任务
func (b *Broker) delay(signature *tasks.Signature, delay time.Duration) error {
	if delay <= 0 {
		return fmt.Errorf("cannot delay task by %v", delay)
	}

	message, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	// 使用 JetStream 的延迟发布功能
	if b.js != nil {
		subject := b.getSubject(signature.RoutingKey)
		_, err = b.js.PublishAsync(subject, message, nats.MsgId(signature.UUID))
		return err
	}

	// 如果没有 JetStream，使用定时器
	go func() {
		time.Sleep(delay)
		subject := b.getSubject(signature.RoutingKey)
		b.conn.Publish(subject, message)
	}()

	return nil
}
