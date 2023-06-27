PROTOS := synapse.proto

.PHONY: all
all: protos

.PHONY: clean
clean: clean-protos

ROOT := $(CURDIR)/../../..
TARGET_SUFFIXES := .pb.go _grpc.pb.go .pb.gw.go _grpc_ext.pb.go

PROTOC_GO_OPTS := \
  --go_opt=Mcommon/proto/dynamic.proto=github.com/KineDB/kinedb-client-go/common/proto

PROTOC_GO_GRPC_OPTS := \
	--go-grpc_opt=Mcommon/proto/dynamic.proto=github.com/KineDB/kinedb-client-go/common/proto

PROTOC_GO_GRPC_EXT_OPTS := \
	--go-grpc-ext_opt=Mcommon/proto/dynamic.proto=github.com/KineDB/kinedb-client-go/common/proto

PROTOC_GO_GRPC_GATEWAY_OPTS := \
	--grpc-gateway_opt=Mcommon/proto/dynamic.proto=github.com/KineDB/kinedb-client-go/common/proto
	
include $(ROOT)/tools/proto/proto.mk