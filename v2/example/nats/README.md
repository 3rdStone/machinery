# NATS 示例

这个示例展示了如何在 Machinery v2 中使用 NATS 作为消息代理。

## 前置要求

1. 安装并运行 NATS 服务器：
   ```bash
   # 使用 Docker
   docker run -p 4222:4222 nats:latest
   
   # 或者下载并安装 NATS 服务器
   # https://github.com/nats-io/nats-server/releases
   ```

2. 安装并运行 Redis 服务器（作为结果后端）：
   ```bash
   # 使用 Docker
   docker run -p 6379:6379 redis:latest
   ```

## 运行示例

### 1. 启动工作器

```bash
cd v2/example/nats
go run main.go worker
```

这将启动一个工作器，等待处理任务。

### 2. 发送任务

在另一个终端中运行：

```bash
cd v2/example/nats
go run main.go send
```

这将发送各种类型的任务到 NATS 队列。

## 功能特性

这个示例展示了以下功能：

- **基本任务**：发送和接收简单的任务
- **延迟任务**：使用 ETA 字段延迟执行任务
- **重试任务**：失败后自动重试的任务
- **组任务**：并行执行多个任务
- **链式任务**：按顺序执行的任务链
- **和弦任务**：等待多个任务完成后执行回调

## 配置选项

NATS 配置支持以下选项：

```go
NATS: &config.NATSConfig{
    URL:           "nats://localhost:4222",  // NATS 服务器地址
    MaxReconnects: 5,                        // 最大重连次数
    ReconnectWait: 2 * time.Second,          // 重连等待时间
    Timeout:       5 * time.Second,          // 连接超时时间
    SubjectPrefix: "machinery",              // 主题前缀
    RetryAttempts: 3,                        // 重试次数
    RetryDelay:    1 * time.Second,          // 重试延迟
}
```

## 性能优势

NATS 相比其他消息代理的优势：

- **高性能**：低延迟、高吞吐量
- **轻量级**：资源占用少
- **简单易用**：API 简洁直观
- **云原生**：适合微服务架构
- **JetStream 支持**：支持消息持久化和流处理

## 故障排除

如果遇到连接问题：

1. 确保 NATS 服务器正在运行
2. 检查端口 4222 是否可访问
3. 验证 NATS 服务器配置
4. 查看日志输出获取详细错误信息

## 更多信息

- [NATS 官方文档](https://docs.nats.io/)
- [Machinery 文档](../README.md)
- [NATS Go 客户端](https://github.com/nats-io/nats.go)
