# lockd

`lockd` 是一个仅依赖 Go 标准库的单节点分布式锁服务，提供公平 FIFO 排队、TTL 租约与自动过期、可重入锁、长轮询通知、SSE 事件、管理员强占、命名空间配额、指标和 Web 控制台。

## 环境

- Go 1.22 或更高版本
- 无外部数据库、Redis、etcd 或前端构建工具依赖

## 构建

```bash
go test -race ./...
go vet ./...
go build -o dist/lockd ./cmd/lockd
go build -o dist/lockctl ./cmd/lockctl
```

## 运行

```bash
./dist/lockd -addr 127.0.0.1:8080 -force-token change-me
```

浏览器打开 `http://127.0.0.1:8080` 可使用内嵌管理控制台。

配置既可通过参数传入，也可使用 `LOCKD_ADDR`、`LOCKD_FORCE_TOKEN`、`LOCKD_NAMESPACES`、`LOCKD_NAMESPACE_QUOTA`、`LOCKD_DEFAULT_TTL`、`LOCKD_EXPIRE_INTERVAL`、`LOCKD_SHUTDOWN_TIMEOUT` 和 `LOCKD_LOG_LEVEL` 环境变量。

## CLI 示例

```bash
./dist/lockctl create -namespace order -name pay -ttl 30s
./dist/lockctl acquire -namespace order -name pay -holder order-svc-1
./dist/lockctl list -namespace order
./dist/lockctl release -namespace order -name pay -token 'tk_...'
```

阻塞获取可增加 `-wait -wait-timeout 10s`。续期、监听、强占和删除分别使用 `renew`、`watch`、`steal`、`delete` 子命令。

## HTTP API

基础路径为 `/api/v1`：

- `POST /locks`、`GET /locks`：创建和列出锁
- `GET|DELETE /locks/{namespace}/{name}`：详情和删除
- `POST /locks/{namespace}/{name}/acquire|renew|release|watch|steal`
- `GET /events`：SSE 事件流
- `GET /metrics`：Prometheus 文本指标
- `GET /healthz`：健康检查

强占和持锁状态下的强制删除必须携带 `X-Force-Token` 请求头。
