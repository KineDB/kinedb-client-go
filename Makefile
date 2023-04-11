.EXPORT_ALL_VARIABLES:

define HELP_INFO
Usage:
        make <Target>

Target:
        all    	build all executables (default)
		web 	build web static files into a go file
        tools   build auxiliary tools
        demos  	build demos
        protos  compile server protobuf files
        servers build servers

        prepare	prepare dependencies
        test    test all packages
        clean   clean artifacts
endef

# Compute dependencies for each command in ./cmds
define CMD_DEPS
$(strip $(if $(1),$(patsubst %,../%,$(filter-out $(1),$(subst github.com/industry-tenebris/kinedb-goclient,.,$(filter github.com/industry-tenebris/kinedb-goclient/%,$(shell go list -deps ./$(1)))))),))
endef

.PHONY: all
all: tools \
	 protos \

.PHONY: prepare
prepare:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go get -u github.com/go-bindata/go-bindata/...

.PHONY: tools
tools: 
	$(MAKE) -C tools/proto tools

.PHONY: clean-tools
clean-tools: 
	$(MAKE) -C tools/proto clean

.PHONY: protos
protos:
	$(MAKE) -C common/proto
	$(MAKE) -C client/api/proto

.PHONY: clean-protos
clean-protos:
	$(MAKE) -C common/proto clean
	$(MAKE) -C client/api/proto clean

bin/%: ./cmd/% .FORCE
	@$(MAKE) -C ./cmd ../$@ "DEPS=$(call CMD_DEPS,$<)"


.PHONY: clean
clean: clean-tools \
       clean-protos \
	  
.PHONY: .FORCE
.FORCE:
