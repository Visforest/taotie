# This is a shortcut to compile protobuf files.

set -e

if ! [ -x "$(command -v protoc)" ];then
    echo 'Error: protoc is not installed. Please go to https://github.com/protocolbuffers/protobuf/releases to install.' >&2
    exit 1
fi

if ! [ -x "$(command -v protoc-gen-go)" ];then
    echo 'protoc-gen-go plugin is not installed, start to install...' >&2
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
    echo 'protoc-gen-go is installed'
fi

if ! [ -x "$(command -v protoc-gen-go-grpc)" ];then
    echo 'protoc-gen-go-grpc plugin is not installed, start to install...' >&2
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
    echo 'protoc-gen-go-grpc is installed'
fi

echo 'Start to compile protobuf files...'
protoc grpc/proto/api.proto -I grpc/proto -I grpc/third_party --go_out=. --go-grpc_out=.
echo 'Done!'