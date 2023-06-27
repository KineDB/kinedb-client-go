package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"

	"github.com/KineDB/kinedb-client-go/agent/etcd"
	"github.com/KineDB/kinedb-client-go/client/api/proto"
	"github.com/KineDB/kinedb-client-go/common/errors"
	"github.com/KineDB/kinedb-client-go/common/model"
	commonProto "github.com/KineDB/kinedb-client-go/common/proto"
	"github.com/KineDB/kinedb-client-go/common/utils"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type Client struct {
	Conn *grpc.ClientConn
}

var sClients = make(map[string]proto.SynapseServiceClient)

func getSClient() proto.SynapseServiceClient {
	synapseServices := etcd.FindServiceByType(etcd.Synapse)
	synapseService := synapseServices[0]
	address := fmt.Sprintf("%v:%v", synapseService.Host, synapseService.Port)
	if !utils.Contains(sClients, address) {
		startSClient(address)
	}
	log.Infof("getSClient %+v", sClients)
	return sClients[address]
}

func ExecuteSQL(ctx context.Context, request model.ExecuteSQLRequest) *commonProto.Results {
	log.Infof("ExecuteSQL sql: %+v", request)
	//sessionId := session.GetSessionId(ctx)
	//log.Infof("ExecuteSQL sql sessionId: {}", sessionId)
	client := getSClient()
	stmt := &commonProto.Statement{Sql: request.Sql, Gql: request.Gql, Engine: request.Engine, Prompt: request.Prompt}
	result, err := client.Execute(ctx, stmt)
	if utils.NotNil(err) {
		panic(err)
	}
	return result
}

func startSClient(address string) {
	client, err := NewClient(address, Tls)
	if err != nil {
		panic(err)
	}

	sClient := proto.NewSynapseServiceRemoteClient(client.Conn)
	sClients[address] = sClient
	log.Infof("Start Synapse Client %+v", address)
}

func NewClient(addr string, tlsConfig TLSConfig) (*Client, errors.Error) {
	return NewClientWithListener(addr, nil, nil, tlsConfig)
}

func NewClientWithListener(addr string, listener *bufconn.Listener, cp grpc.UnaryClientInterceptor, tlsConfig TLSConfig) (*Client, errors.Error) {
	c := &Client{}

	var opts []grpc.DialOption

	if tlsConfig.Enabled {
		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM([]byte(tlsConfig.Cert)) {
			return nil, errors.Newf(errors.GenericUnknownError, "unable to create cert pool")
		}

		creds := credentials.NewTLS(&tls.Config{
			ServerName: addr,
			RootCAs:    certPool,
		})

		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	if cp != nil {
		opts = append(opts, grpc.WithChainUnaryInterceptor(cp))
	}

	if listener != nil {
		opts = append(opts, grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	}

	var err error
	c.Conn, err = grpc.Dial(addr, opts...)
	if err != nil {
		return nil, errors.Newf(errors.GenericUnknownError, "unable to dial: %+v", err)
	}

	return c, nil
}
