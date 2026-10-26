FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/quorumd ./cmd/quorumd \
 && CGO_ENABLED=0 GOOS=linux go build -o /out/quorumctl ./cmd/quorumctl \
 && CGO_ENABLED=0 GOOS=linux go build -o /out/scheduler-demo ./cmd/scheduler-demo \
 && CGO_ENABLED=0 GOOS=linux go build -o /out/settlement-bench ./cmd/settlement-bench

FROM gcr.io/distroless/static-debian12 AS runtime
COPY --from=build /out/quorumd /quorumd
ENTRYPOINT ["/quorumd"]

FROM debian:bookworm-slim AS tools
COPY --from=build /out/quorumctl /quorumctl
COPY --from=build /out/scheduler-demo /scheduler-demo
COPY --from=build /out/settlement-bench /settlement-bench
CMD ["/bin/sh"]
