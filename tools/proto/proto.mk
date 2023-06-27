GOPATH=$(shell go env GOPATH)

ifndef ROOT
$(error ROOT is not defined.)
endif

ifndef TARGET_SUFFIXES
  TARGET_SUFFIXES := .pb.go .pb.gw.go _grpc.pb.go _grpc_ext.pb.go
endif
TARGETS := $(foreach suffix,$(TARGET_SUFFIXES),$(foreach proto,$(PROTOS),$(basename $(proto))$(suffix)))

.PHONY: protos
protos: $(TARGETS)

%.pb.go: %.proto
	protoc \
	  -I"$(CURDIR)" \
	  -I"$(ROOT)" \
	  -I"$(ROOT)/third-party/protobuf" \
	  --go_out="$(ROOT)" \
	  $(PROTOC_GO_OPTS) \
	  $<

%.pb.gw.go: %.proto
	protoc \
	  -I"$(CURDIR)" \
	  -I"$(ROOT)" \
	  -I"$(ROOT)/third-party/protobuf" \
	  --grpc-gateway_out="$(ROOT)" \
	  $(PROTOC_GO_GRPC_GATEWAY_OPTS) \
	  $<

%_grpc.pb.go: %.proto
	protoc \
	  -I"$(CURDIR)" \
	  -I"$(ROOT)" \
	  -I"$(ROOT)/third-party/protobuf" \
	  --go-grpc_opt=require_unimplemented_servers=false \
	  --go-grpc_out="$(ROOT)" \
	  $(PROTOC_GO_GRPC_OPTS) \
	  $<

%_grpc_ext.pb.go: %.proto
	PATH="$(ROOT)/tools/proto:$$PATH" \
	protoc \
	  -I"$(CURDIR)" \
	  -I"$(ROOT)" \
	  -I"$(ROOT)/third-party/protobuf" \
	  --go-grpc-ext_opt=channel_buffer=10 \
	  --go-grpc-ext_out="$(ROOT)" \
	  $(PROTOC_GO_GRPC_EXT_OPTS) \
	  $<

.PHONY: clean-protos
clean-protos:
	rm -f $(TARGETS)
