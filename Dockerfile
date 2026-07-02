FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/node ./cmd/node
RUN CGO_ENABLED=0 go build -o /out/kvctl ./cmd/kvctl

FROM alpine:3.20
COPY --from=build /out/node /usr/local/bin/node
COPY --from=build /out/kvctl /usr/local/bin/kvctl
ENTRYPOINT ["node"]
