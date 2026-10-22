FROM golang:1.22 AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/quorumd ./cmd/quorumd

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/quorumd /quorumd
ENTRYPOINT ["/quorumd"]

