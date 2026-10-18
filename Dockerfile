# syntax=docker/dockerfile:1

# The build stage runs on the builder's own platform and cross-compiles, so an
# arm64 image builds at native speed on an amd64 runner instead of under QEMU.
FROM --platform=$BUILDPLATFORM golang:1.27-alpine@sha256:f92b6ef800e499660581efdabdf25d9d817a9d124eaf900924f0504e7e27e12d AS build

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
        -o /out/beastmux ./cmd/beastmux

FROM scratch

COPY --from=build /out/beastmux /beastmux

# Numeric, so Kubernetes' runAsNonRoot check can verify it without a passwd
# lookup. A fixed id also lets a deployment's runAsUser, runAsGroup and
# volume ownership be set to match the image.
USER 10001:10001
EXPOSE 30005
ENTRYPOINT ["/beastmux"]
