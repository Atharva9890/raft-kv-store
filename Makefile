.PHONY: build test up down logs kill-leader proto

build:
	go build ./...

test:
	go test ./...

proto:
	protoc --go_out=. --go-grpc_out=. proto/kv.proto
	mkdir -p proto/kvpb
	mv github.com/Atharva9890/raft-kv-store/proto/kvpb/*.go proto/kvpb/
	rm -rf github.com

up:
	docker compose up --build

down:
	docker compose down

logs:
	docker compose logs -f

kill-leader:
	./scripts/kill_leader.sh
