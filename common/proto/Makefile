PROTOS := dynamic.proto

.PHONY: all
all: protos

.PHONY: clean
clean: clean-protos

ROOT := $(CURDIR)/../..
TARGET_SUFFIXES := .pb.go
include $(ROOT)/tools/proto/proto.mk
