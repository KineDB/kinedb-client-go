package main

import (
	"flag"
	"fmt"
	"strings"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

const (
	version = "1.0.0"

	deprecationComment = "// Deprecated: Do not use."

	contextPackage = protogen.GoImportPath("context")
	errorsPackage  = protogen.GoImportPath("github.com/KineDB/kinedb-client-go/common/errors")
	grpcPackage    = protogen.GoImportPath("google.golang.org/grpc")
	ioPackage      = protogen.GoImportPath("io")
	logrusPackage  = protogen.GoImportPath("github.com/sirupsen/logrus")
	servicePackage = protogen.GoImportPath("github.com/KineDB/kinedb-client-go/common/service")
)

func protocVersion(gen *protogen.Plugin) string {
	v := gen.Request.GetCompilerVersion()
	if v == nil {
		return "(unknown)"
	}
	var suffix string
	if s := v.GetSuffix(); s != "" {
		suffix = "-" + s
	}
	return fmt.Sprintf("v%d.%d.%d%s", v.GetMajor(), v.GetMinor(), v.GetPatch(), suffix)
}

func formatFullMethodName(service *protogen.Service, method *protogen.Method) string {
	return fmt.Sprintf("/%s/%s", service.Desc.FullName(), method.Desc.Name())
}

func unexport(s string) string { return strings.ToLower(s[:1]) + s[1:] }

func generateClientSignature(g *protogen.GeneratedFile, method *protogen.Method) string {
	s := method.GoName + "(ctx " + g.QualifiedGoIdent(contextPackage.Ident("Context"))
	if !method.Desc.IsStreamingClient() {
		s += ", in *" + g.QualifiedGoIdent(method.Input.GoIdent)
	}
	s += ", opts ..." + g.QualifiedGoIdent(grpcPackage.Ident("CallOption")) + ") ("
	if !method.Desc.IsStreamingClient() && !method.Desc.IsStreamingServer() {
		s += "*" + g.QualifiedGoIdent(method.Output.GoIdent)
	} else {
		s += method.Parent.GoName + "_" + method.GoName + "Client"
	}
	s += ", error)"
	return s
}

func generateLocalClientStruct(g *protogen.GeneratedFile, clientName, serverType string) {
	g.P("type ", unexport(clientName), " struct {")
	g.P("s ", serverType)
	g.P("}")
	g.P()
}

func generateRemoteClientStruct(g *protogen.GeneratedFile, clientName, clientInterfaceName string) {
	g.P("type ", unexport(clientName), " struct {")
	g.P("c ", clientInterfaceName)
	g.P("}")
	g.P()
}

func generateNewLocalClientDefinitions(g *protogen.GeneratedFile, clientName string) {
	g.P("return &", unexport(clientName), "{s}")
}

func generateNewRemoteClientDefinitions(g *protogen.GeneratedFile, clientName, clientInterfaceName string) {
	g.P("return &", unexport(clientName), "{New", clientInterfaceName, "(cc)}")
}

func generateFile(gen *protogen.Plugin, file *protogen.File) *protogen.GeneratedFile {
	if len(file.Services) == 0 {
		return nil
	}
	filename := file.GeneratedFilenamePrefix + "_grpc_ext.pb.go"
	g := gen.NewGeneratedFile(filename, file.GoImportPath)
	g.P("// Code generated by protoc-gen-go-grpc. DO NOT EDIT.")
	g.P("// versions:")
	g.P("// - protoc-gen-go-grpc v", version)
	g.P("// - protoc             ", protocVersion(gen))
	if file.Proto.GetOptions().GetDeprecated() {
		g.P("// ", file.Desc.Path(), " is a deprecated file.")
	} else {
		g.P("// source: ", file.Desc.Path())
	}
	g.P()
	g.P("package ", file.GoPackageName)
	g.P()
	generateFileContent(gen, file, g)
	return g
}

func generateFileContent(gen *protogen.Plugin, file *protogen.File, g *protogen.GeneratedFile) {
	if len(file.Services) == 0 {
		return
	}

	g.P("// This is a compile-time assertion to ensure that this generated file")
	g.P("// is compatible with the grpc package it is being compiled against.")
	g.P("// Requires gRPC-Go v1.32.0 or later.")
	g.P("const _ = ", grpcPackage.Ident("SupportPackageIsVersion7")) // When changing, update version number above.
	g.P()
	for _, service := range file.Services {
		generateService(gen, file, g, service)
	}
}

func generateLocalClientMethod(gen *protogen.Plugin, file *protogen.File, g *protogen.GeneratedFile, method *protogen.Method, index int) {
	service := method.Parent

	if method.Desc.Options().(*descriptorpb.MethodOptions).GetDeprecated() {
		g.P(deprecationComment)
	}
	g.P("func (c *", unexport(service.GoName), "LocalClient) ", generateClientSignature(g, method), "{")
	if !method.Desc.IsStreamingServer() && !method.Desc.IsStreamingClient() {
		g.P("m, err := c.s.", method.GoName, "(ctx, in)")
		g.P(servicePackage.Ident("ProcessLocalOpts"), "(ctx, opts...)")
		g.P("if err != nil {")
		g.P(logrusPackage.Ident("Errorf"), "(\"%+v\\n\", err)")
		g.P("}")
		g.P("return m, err")
		g.P("}")
		g.P()
		return
	}
	streamType := unexport(service.GoName) + method.GoName + "ServerExt"
	localStreamType := unexport(service.GoName) + method.GoName + "LocalClient"
	g.P("channel := ", servicePackage.Ident("NewStreamChannel"), fmt.Sprintf("(%d)", *channelBufferSize))
	g.P("serverStream := ", servicePackage.Ident("NewLocalServerStream"), "(ctx, channel)")
	g.P("tmp := make(chan bool)")
	g.P("go func() {")
	g.P("tmp <- true")
	g.P(servicePackage.Ident("ProcessLocalOpts"), "(ctx, opts...)")
	if !method.Desc.IsStreamingClient() {
		g.P("if err := c.s.", method.GoName, "(in, ", "&", streamType, "{serverStream}); err != nil {")
	} else {
		g.P("if err := c.s.", method.GoName, "(", "&", streamType, "{serverStream}); err != nil {")
	}
	g.P("channel.Error(err)")
	g.P("} else {")
	g.P("channel.SendToClient(", ioPackage.Ident("EOF"), ")")
	g.P("channel.SendToServer(", ioPackage.Ident("EOF"), ")")
	g.P("}")
	g.P("}()")
	g.P("<-tmp")
	g.P("return &", localStreamType, "{", servicePackage.Ident("NewLocalClientStream"), "(ctx, channel)}, nil")
	g.P("}")
	g.P()

	genSend := method.Desc.IsStreamingClient()
	genRecv := method.Desc.IsStreamingServer()
	genCloseAndRecv := !method.Desc.IsStreamingServer()

	// Stream auxiliary types and methods.
	g.P("type ", localStreamType, " struct {")
	g.P(grpcPackage.Ident("ClientStream"))
	g.P("}")
	g.P()

	if genSend {
		g.P("func (x *", localStreamType, ") Send(m *", method.Input.GoIdent, ") error {")
		g.P("err := x.ClientStream.SendMsg(m)")
		g.P("return ", errorsPackage.Ident("ConvertSynapseExceptionToGrpcError"), "(err)")
		g.P("}")
		g.P()
	}
	if genRecv {
		g.P("func (x *", localStreamType, ") Recv() (*", method.Output.GoIdent, ", error) {")
		g.P("m := new(", method.Output.GoIdent, ")")
		g.P("if err := x.ClientStream.RecvMsg(m); err != nil { return nil, ", errorsPackage.Ident("ConvertSynapseExceptionToGrpcError"), "(err)", " }")
		g.P("return m, nil")
		g.P("}")
		g.P()
	}
	if genCloseAndRecv {
		g.P("func (x *", streamType, ") CloseAndRecv() (*", method.Output.GoIdent, ", error) {")
		g.P("if err := x.ClientStream.CloseSend(); err != nil { return nil, ", errorsPackage.Ident("ConvertSynapseExceptionToGrpcError"), "(err)", " }")
		g.P("m := new(", method.Output.GoIdent, ")")
		g.P("if err := x.ClientStream.RecvMsg(m); err != nil { return nil, ", errorsPackage.Ident("ConvertSynapseExceptionToGrpcError"), "(err)", " }")
		g.P("return m, nil")
		g.P("}")
		g.P()
	}
}

func generateRemoteClientMethod(gen *protogen.Plugin, file *protogen.File, g *protogen.GeneratedFile, method *protogen.Method, index int) {
	service := method.Parent

	if method.Desc.Options().(*descriptorpb.MethodOptions).GetDeprecated() {
		g.P(deprecationComment)
	}
	g.P("func (c *", unexport(service.GoName), "RemoteClient) ", generateClientSignature(g, method), "{")
	if !method.Desc.IsStreamingServer() && !method.Desc.IsStreamingClient() {
		g.P("m, err := c.c.", method.GoName, "(ctx, in, opts...)")
		g.P("return m, ", errorsPackage.Ident("ConvertSynapseExceptionToGrpcError"), "(err)")
		g.P("}")
		g.P()
		return
	}
	streamType := unexport(service.GoName) + method.GoName + "Client"
	remoteStreamType := unexport(service.GoName) + method.GoName + "RemoteClient"
	if !method.Desc.IsStreamingClient() {
		g.P("x, err := c.c.", method.GoName, "(ctx, in, opts...)")
	} else {
		g.P("x, err := c.c.", method.GoName, "(ctx, opts...)")
	}
	g.P("return &", remoteStreamType, "{x.(*", streamType, "), x.(*", streamType, ").ClientStream}, ", errorsPackage.Ident("ConvertSynapseExceptionToGrpcError"), "(err)")
	g.P("}")
	g.P()

	genSend := method.Desc.IsStreamingClient()
	genRecv := method.Desc.IsStreamingServer()
	genCloseAndRecv := !method.Desc.IsStreamingServer()

	// Stream auxiliary types and methods.
	g.P("type ", remoteStreamType, " struct {")
	g.P("x *", streamType)
	g.P(grpcPackage.Ident("ClientStream"))
	g.P("}")
	g.P()

	if genSend {
		g.P("func (x *", remoteStreamType, ") Send(m *", method.Input.GoIdent, ") error {")
		g.P("err := x.x.Send(m)")
		g.P("return ", errorsPackage.Ident("ConvertSynapseExceptionToGrpcError"), "(err)")
		g.P("}")
		g.P()
	}
	if genRecv {
		g.P("func (x *", remoteStreamType, ") Recv() (*", method.Output.GoIdent, ", error) {")
		g.P("m, err := x.x.Recv()")
		g.P("return m, ", errorsPackage.Ident("ConvertSynapseExceptionToGrpcError"), "(err)")
		g.P("}")
		g.P()
	}
	if genCloseAndRecv {
		g.P("func (x *", remoteStreamType, ") CloseAndRecv() (*", method.Output.GoIdent, ", error) {")
		g.P("m, err := x.x.CloseAndRecv()")
		g.P("return m, ", errorsPackage.Ident("ConvertSynapseExceptionToGrpcError"), "(err)")
		g.P("}")
		g.P()
	}
}

func generateServerMethod(gen *protogen.Plugin, file *protogen.File, g *protogen.GeneratedFile, method *protogen.Method, hnameFuncNameFormatter func(string) string) string {
	service := method.Parent
	hname := fmt.Sprintf("_%s_%s_HandlerExt", service.GoName, method.GoName)

	if !method.Desc.IsStreamingClient() && !method.Desc.IsStreamingServer() {
		return hname
	}
	streamType := unexport(service.GoName) + method.GoName + "ServerExt"
	g.P("func ", hnameFuncNameFormatter(hname), "(srv interface{}, stream ", grpcPackage.Ident("ServerStream"), ") error {")
	if !method.Desc.IsStreamingClient() {
		g.P("m := new(", method.Input.GoIdent, ")")
		g.P("if err := stream.RecvMsg(m); err != nil { return err }")
		g.P("return srv.(", service.GoName, "Server).", method.GoName, "(m, &", streamType, "{stream})")
	} else {
		g.P("return srv.(", service.GoName, "Server).", method.GoName, "(&", streamType, "{stream})")
	}
	g.P("}")
	g.P()

	genSend := method.Desc.IsStreamingServer()
	genSendAndClose := !method.Desc.IsStreamingServer()
	genRecv := method.Desc.IsStreamingClient()

	g.P("type ", streamType, " struct {")
	g.P(grpcPackage.Ident("ServerStream"))
	g.P("}")
	g.P()

	if genSend {
		g.P("func (x *", streamType, ") Send(m *", method.Output.GoIdent, ") error {")
		g.P("err := x.ServerStream.SendMsg(m)")
		g.P("return ", errorsPackage.Ident("ConvertSynapseExceptionToGrpcError"), "(err)")
		g.P("}")
		g.P()
	}
	if genSendAndClose {
		g.P("func (x *", streamType, ") SendAndClose(m *", method.Output.GoIdent, ") error {")
		g.P("err := x.ServerStream.SendMsg(m)")
		g.P("return ", errorsPackage.Ident("ConvertSynapseExceptionToGrpcError"), "(err)")
		g.P("}")
		g.P()
	}
	if genRecv {
		g.P("func (x *", streamType, ") Recv() (*", method.Input.GoIdent, ", error) {")
		g.P("m := new(", method.Input.GoIdent, ")")
		g.P("if err := x.ServerStream.RecvMsg(m); err != nil { return nil, ", errorsPackage.Ident("ConvertSynapseExceptionToGrpcError"), "(err)}")
		g.P("return m, nil")
		g.P("}")
		g.P()
	}

	return hname
}

func generateServerFunctions(gen *protogen.Plugin, file *protogen.File, g *protogen.GeneratedFile, service *protogen.Service, serverType string, serviceDescVar string) {
	// Server handler implementations.
	handlerNames := make([]string, 0, len(service.Methods))
	for _, method := range service.Methods {
		hname := generateServerMethod(gen, file, g, method, func(hname string) string {
			return hname
		})
		handlerNames = append(handlerNames, hname)
	}

	// Reset handlers
	if len(handlerNames) > 0 {
		g.P("func init() {")
		idx := 0
		for i, method := range service.Methods {
			if !method.Desc.IsStreamingClient() && !method.Desc.IsStreamingServer() {
				continue
			}
			g.P(serviceDescVar, ".Streams[", idx, "].Handler = ", handlerNames[i])
			idx++
		}
		g.P("}")
	}
}

func generateService(gen *protogen.Plugin, file *protogen.File, g *protogen.GeneratedFile, service *protogen.Service) {
	clientInterfaceName := service.GoName + "Client"
	//localClientStructName := service.GoName + "LocalClient"
	remoteClientStructName := service.GoName + "RemoteClient"
	serverType := service.GoName + "Server"

	// Client structure.
	//generateLocalClientStruct(g, localClientStructName, serverType)
	generateRemoteClientStruct(g, remoteClientStructName, clientInterfaceName)

	// Local NewClient factory.
	if service.Desc.Options().(*descriptorpb.ServiceOptions).GetDeprecated() {
		g.P(deprecationComment)
	}
	// g.P("func New", localClientStructName, " (s ", serverType, ") ", clientInterfaceName, " {")
	// generateNewLocalClientDefinitions(g, localClientStructName)
	// g.P("}")
	// g.P()

	var methodIndex, streamIndex int
	// Local client method implementations.
	// for _, method := range service.Methods {
	// 	if !method.Desc.IsStreamingServer() && !method.Desc.IsStreamingClient() {
	// 		// Unary RPC method
	// 		generateLocalClientMethod(gen, file, g, method, methodIndex)
	// 		methodIndex++
	// 	} else {
	// 		// Streaming RPC method
	// 		generateLocalClientMethod(gen, file, g, method, streamIndex)
	// 		streamIndex++
	// 	}
	// }

	g.P("func New", remoteClientStructName, " (cc ", grpcPackage.Ident("ClientConnInterface"), ") ", clientInterfaceName, " {")
	generateNewRemoteClientDefinitions(g, remoteClientStructName, clientInterfaceName)
	g.P("}")
	g.P()

	// Remote client method implementations.
	for _, method := range service.Methods {
		if !method.Desc.IsStreamingServer() && !method.Desc.IsStreamingClient() {
			// Unary RPC method
			generateRemoteClientMethod(gen, file, g, method, methodIndex)
			methodIndex++
		} else {
			// Streaming RPC method
			generateRemoteClientMethod(gen, file, g, method, streamIndex)
			streamIndex++
		}
	}

	serviceDescVar := service.GoName + "_ServiceDesc"
	generateServerFunctions(gen, file, g, service, serverType, serviceDescVar)
}

var channelBufferSize *int

func main() {
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Printf("protoc-gen-go-grpc %+v\n", version)
		return
	}

	var flags flag.FlagSet
	channelBufferSize = flags.Int("channel_buffer", 256, "the channel buffer size for local streaming (default 256)")

	protogen.Options{
		ParamFunc: flags.Set,
	}.Run(func(gen *protogen.Plugin) error {
		gen.SupportedFeatures = uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL)
		for _, f := range gen.Files {
			if !f.Generate {
				continue
			}
			generateFile(gen, f)
		}
		return nil
	})
}
