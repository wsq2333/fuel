# Fuel 镜像（纯 Go 静态二进制，无 CGO——AGENTS.md §3.5）
#
# 构建:
#   CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o fuel ./cmd/fuel
#   docker build -f deploy/Dockerfile -t registry.example.com/fuel:dev .
FROM scratch
COPY fuel /fuel
ENTRYPOINT ["/fuel"]
