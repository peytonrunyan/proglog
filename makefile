compile:
        protoc api/v1/*.proto \
test:
        --go_out=. \
        --go_opt=paths=source_relative \
        --proto_path=.
go test -race ./...