package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/RichardKnop/machinery/v2"
	backendsiface "github.com/RichardKnop/machinery/v2/backends/iface"
	"github.com/RichardKnop/machinery/v2/backends/redis"
	brokersiface "github.com/RichardKnop/machinery/v2/brokers/iface"
	"github.com/RichardKnop/machinery/v2/brokers/nats"
	"github.com/RichardKnop/machinery/v2/config"
	locksiface "github.com/RichardKnop/machinery/v2/locks/iface"
	redislock "github.com/RichardKnop/machinery/v2/locks/redis"
	"github.com/RichardKnop/machinery/v2/tasks"
)

var (
	broker  brokersiface.Broker
	backend backendsiface.Backend
	lock    locksiface.Lock
	cnf     *config.Config
	server  *machinery.Server
	worker  *machinery.Worker
)

func init() {
	// 创建配置
	cnf = &config.Config{
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

	// 创建 broker
	broker = nats.New(cnf)

	// 创建 backend
	backend = redis.New(cnf, "localhost", "", "", 0)

	// 创建 lock
	lock = redislock.New(cnf, []string{"localhost:6379"}, 0, 3)

	// 创建服务器
	server = machinery.NewServer(cnf, broker, backend, lock)
}

func main() {
	// 解析命令行参数
	flag.Parse()
	args := flag.Args()

	if len(args) < 1 {
		log.Fatal("请指定操作: worker 或 send")
	}

	command := args[0]

	switch command {
	case "worker":
		startWorker()
	case "send":
		sendTasks()
	default:
		log.Fatal("未知命令:", command)
	}
}

// startWorker 启动工作器
func startWorker() {
	// 注册任务
	server.RegisterTasks(map[string]interface{}{
		"add":        add,
		"multiply":   multiply,
		"panic_task": panicTask,
	})

	// 创建工作器
	worker = server.NewWorker("machinery_worker", 10)

	// 设置信号处理
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// 启动工作器
	go func() {
		if err := worker.Launch(); err != nil {
			log.Fatal("启动工作器失败:", err)
		}
	}()

	// 等待退出信号
	<-quit
	log.Println("正在关闭工作器...")

	// 停止工作器
	worker.Quit()
	log.Println("工作器已停止")
}

// sendTasks 发送任务
func sendTasks() {
	// 注册任务（发送端也需要注册）
	server.RegisterTasks(map[string]interface{}{
		"add":        add,
		"multiply":   multiply,
		"panic_task": panicTask,
	})

	// 发送单个任务
	sendSingleTask()

	// 发送延迟任务
	sendDelayedTask()

	// 发送重试任务
	sendRetryTask()

	// 发送组任务
	sendGroupTasks()

	// 发送链式任务
	sendChainTasks()

	// 发送和弦任务
	sendChordTasks()
}

// sendSingleTask 发送单个任务
func sendSingleTask() {
	signature := &tasks.Signature{
		Name: "add",
		Args: []tasks.Arg{
			{
				Type:  "int64",
				Value: 1,
			},
			{
				Type:  "int64",
				Value: 1,
			},
		},
	}

	asyncResult, err := server.SendTask(signature)
	if err != nil {
		log.Fatal("发送任务失败:", err)
	}

	// 等待结果
	results, err := asyncResult.Get(time.Duration(time.Millisecond * 5))
	if err != nil {
		log.Fatal("获取结果失败:", err)
	}

	log.Printf("任务结果: %v", results[0].Interface())
}

// sendDelayedTask 发送延迟任务
func sendDelayedTask() {
	eta := time.Now().UTC().Add(time.Second * 5)
	signature := &tasks.Signature{
		Name: "multiply",
		Args: []tasks.Arg{
			{
				Type:  "int64",
				Value: 4,
			},
			{
				Type:  "int64",
				Value: 4,
			},
		},
		ETA: &eta,
	}

	asyncResult, err := server.SendTask(signature)
	if err != nil {
		log.Fatal("发送延迟任务失败:", err)
	}

	log.Println("延迟任务已发送，将在5秒后执行")

	// 等待结果
	results, err := asyncResult.Get(time.Duration(time.Second * 10))
	if err != nil {
		log.Fatal("获取延迟任务结果失败:", err)
	}

	log.Printf("延迟任务结果: %v", results[0].Interface())
}

// sendRetryTask 发送重试任务
func sendRetryTask() {
	signature := &tasks.Signature{
		Name:         "panic_task",
		Args:         []tasks.Arg{},
		RetryCount:   3,
		RetryTimeout: 1,
	}

	asyncResult, err := server.SendTask(signature)
	if err != nil {
		log.Fatal("发送重试任务失败:", err)
	}

	log.Println("重试任务已发送")

	// 等待结果
	results, err := asyncResult.Get(time.Duration(time.Second * 10))
	if err != nil {
		log.Printf("重试任务最终失败: %v", err)
	} else {
		log.Printf("重试任务结果: %v", results[0].Interface())
	}
}

// sendGroupTasks 发送组任务
func sendGroupTasks() {
	signature1 := &tasks.Signature{
		Name: "add",
		Args: []tasks.Arg{
			{Type: "int64", Value: 1},
			{Type: "int64", Value: 1},
		},
	}

	signature2 := &tasks.Signature{
		Name: "add",
		Args: []tasks.Arg{
			{Type: "int64", Value: 5},
			{Type: "int64", Value: 5},
		},
	}

	group, _ := tasks.NewGroup(signature1, signature2)
	asyncResults, err := server.SendGroup(group, 0)
	if err != nil {
		log.Fatal("发送组任务失败:", err)
	}

	log.Println("组任务已发送")

	// 等待所有结果
	for i, asyncResult := range asyncResults {
		results, err := asyncResult.Get(time.Duration(time.Second * 5))
		if err != nil {
			log.Printf("获取组任务 %d 结果失败: %v", i, err)
		} else {
			log.Printf("组任务 %d 结果: %v", i, results[0].Interface())
		}
	}
}

// sendChainTasks 发送链式任务
func sendChainTasks() {
	signature1 := &tasks.Signature{
		Name: "add",
		Args: []tasks.Arg{
			{Type: "int64", Value: 1},
			{Type: "int64", Value: 1},
		},
	}

	signature2 := &tasks.Signature{
		Name: "add",
		Args: []tasks.Arg{
			{Type: "int64", Value: 5},
			{Type: "int64", Value: 5},
		},
	}

	signature3 := &tasks.Signature{
		Name: "multiply",
		Args: []tasks.Arg{
			{Type: "int64", Value: 4},
		},
	}

	chain, _ := tasks.NewChain(signature1, signature2, signature3)
	asyncResult, err := server.SendChain(chain)
	if err != nil {
		log.Fatal("发送链式任务失败:", err)
	}

	log.Println("链式任务已发送")

	// 等待结果
	results, err := asyncResult.Get(time.Duration(time.Second * 10))
	if err != nil {
		log.Fatal("获取链式任务结果失败:", err)
	}

	log.Printf("链式任务结果: %v", results[0].Interface())
}

// sendChordTasks 发送和弦任务
func sendChordTasks() {
	signature1 := &tasks.Signature{
		Name: "add",
		Args: []tasks.Arg{
			{Type: "int64", Value: 1},
			{Type: "int64", Value: 1},
		},
	}

	signature2 := &tasks.Signature{
		Name: "add",
		Args: []tasks.Arg{
			{Type: "int64", Value: 5},
			{Type: "int64", Value: 5},
		},
	}

	signature3 := &tasks.Signature{
		Name: "multiply",
	}

	group, _ := tasks.NewGroup(signature1, signature2)
	chord, _ := tasks.NewChord(group, signature3)
	asyncResult, err := server.SendChord(chord, 0)
	if err != nil {
		log.Fatal("发送和弦任务失败:", err)
	}

	log.Println("和弦任务已发送")

	// 等待结果
	results, err := asyncResult.Get(time.Duration(time.Second * 10))
	if err != nil {
		log.Fatal("获取和弦任务结果失败:", err)
	}

	log.Printf("和弦任务结果: %v", results[0].Interface())
}

// 任务函数定义

// add 加法任务
func add(args ...int64) (int64, error) {
	sum := int64(0)
	for _, arg := range args {
		sum += arg
	}
	log.Printf("执行加法任务: %v = %d", args, sum)
	return sum, nil
}

// multiply 乘法任务
func multiply(args ...int64) (int64, error) {
	sum := int64(1)
	for _, arg := range args {
		sum *= arg
	}
	log.Printf("执行乘法任务: %v = %d", args, sum)
	return sum, nil
}

// panicTask 会失败的任务（用于测试重试）
func panicTask() error {
	log.Println("执行会失败的任务")
	return fmt.Errorf("这是一个测试错误")
}
