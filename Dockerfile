# Build the manager binary
FROM golang:1.18 as builder

WORKDIR /workspace
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# and so that source changes don't invalidate our downloaded layer

# Copy the go source
COPY cmd cmd
COPY pkg pkg


# Build
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o kafkatest ./cmd/kafkatest

# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/kafkatest .
USER 65532:65532

ENTRYPOINT ["/kafkatest"]