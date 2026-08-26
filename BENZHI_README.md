基于 Go 实现的分布式锁 Web 服务项目，一款后端服务，提供公平排队、TTL 租约、可重入锁、自动续期、HTTP API、CLI 与内嵌管理界面。

# lockd 评测说明

## 项目类型

Go 后端服务 + 原生 HTML/CSS/JavaScript 内嵌前端，不依赖数据库、Redis、etcd、Node.js 或第三方 Go 模块。

## 本地构建

```bash
go build ./...
go build -o dist/lockd ./cmd/lockd
go build -o dist/lockctl ./cmd/lockctl
```

## 本地运行

```bash
go run ./cmd/lockd -addr 127.0.0.1:8080 -force-token change-me
```

启动后访问：

```text
http://127.0.0.1:8080
```

健康检查：

```bash
curl -fsS http://127.0.0.1:8080/api/v1/healthz
```

## 测试与静态检查

```bash
go test ./...
go test -race ./...
go vet ./...
```

## Docker 评测镜像

```bash
chmod +x build_benzhi_docker.sh
./build_benzhi_docker.sh lockd linux/arm64
./build_benzhi_docker.sh lockd linux/amd64
docker run --rm -it lockd:latest
```

容器保留完整 Go 工具链，进入容器后可再次运行：

```bash
go build ./...
go test ./...
```
