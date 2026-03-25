## Build
FROM registry.access.redhat.com/ubi9/go-toolset:1.25 AS build

USER root

ADD . /app

RUN cd /app && CGO_ENABLED=0 go build -ldflags='-extldflags=-static' -o=projection ./cmd/consumer/main.go

RUN cd /app && CGO_ENABLED=0 go build -ldflags='-extldflags=-static' -o=onprem ./cmd/onprem/main.go

RUN cd /app && CGO_ENABLED=0 go build -ldflags='-extldflags=-static' -o=migrations ./cmd/migrations/main.go

## Licenses
FROM registry.access.redhat.com/ubi9/go-toolset:1.25 AS licenses

ADD . /app
WORKDIR /app

RUN go install github.com/google/go-licenses@v1.6.0
RUN ${HOME}/go/bin/go-licenses save --save_path /tmp/licenses ./...

## Runtime
FROM registry.access.redhat.com/ubi9/ubi-minimal:9.5

ARG release=main
ARG version=latest

LABEL com.redhat.component assisted-events-stream
LABEL description "Pipeline processors for assisted installer service events"
LABEL summary "Pipeline processors for assisted installer service events"
LABEL io.k8s.description "Pipeline processors for assisted installer service events"
LABEL distribution-scope public
LABEL name assisted-events-stream
LABEL release ${release}
LABEL version ${version}
LABEL url https://github.com/openshift-assisted/assisted-events-stream
LABEL vendor "Red Hat, Inc."
LABEL maintainer "Red Hat"

COPY --from=licenses /tmp/licenses /licenses

COPY --from=build /app/projection /
COPY --from=build /app/onprem /
COPY --from=build /app/migrations /

USER 1001:1001

ENTRYPOINT ["/projection"]
