# syntax=docker/dockerfile:1

# The build stage runs on the builder's own platform and cross-compiles, so an
# arm64 image builds at native speed on an amd64 runner instead of under QEMU.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine@sha256:3082400e369fa24d5fc60bca20edab3f6d604e0c5a690ec66b295eff4dd87ade AS build

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

# Scratch, not distroless: the binary is static, speaks plain TCP and loads no
# time zone, so CA certificates, tzdata and a passwd file would be dead weight
# with a digest to keep current and CVE reports to wave away.
FROM scratch

COPY --from=build /out/beastmux /beastmux

# Numeric, so Kubernetes' runAsNonRoot check can verify it without a passwd
# lookup. A fixed id also lets a deployment's runAsUser, runAsGroup and
# volume ownership be set to match the image.
USER 10001:10001
EXPOSE 30005
ENTRYPOINT ["/beastmux"]
