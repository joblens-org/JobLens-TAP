.PHONY: build run clean test lint

# 变量
BINARY_NAME=server
BUILD_DIR=./cmd/server

# 版本信息（Version 在 main.go 中人为维护）
GIT_COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME ?= $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')
LDFLAGS    = -s -w -X main.GitCommit=$(GIT_COMMIT) -X main.BuildTime=$(BUILD_TIME)

# 默认目标
all: build

# 构建
build:
	@echo "Building..."
	go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY_NAME) $(BUILD_DIR)

# 运行（本地开发）
run:
	@echo "Running..."
	go run $(BUILD_DIR)

# 清理
 clean:
	@echo "Cleaning..."
	rm -rf bin/
	go clean

# 测试
test:
	@echo "Running tests..."
	go test -v ./...

# 测试覆盖率
test-coverage:
	@echo "Running tests with coverage..."
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# 代码格式化
fmt:
	@echo "Formatting code..."
	go fmt ./...

# 代码检查
lint:
	@echo "Linting code..."
	golangci-lint run ./...

# 下载依赖
deps:
	@echo "Downloading dependencies..."
	go mod download
	go mod tidy

# 生成（如果有代码生成需求）
generate:
	@echo "Generating code..."
	go generate ./...

# 帮助
help:
	@echo "Available targets:"
	@echo "  build        - Build the binary"
	@echo "  run          - Run the application locally"
	@echo "  clean        - Clean build artifacts"
	@echo "  test         - Run tests"
	@echo "  test-coverage- Run tests with coverage"
	@echo "  fmt          - Format code"
	@echo "  lint         - Run linter"
	@echo "  deps         - Download dependencies"
	@echo "  help         - Show this help message"
