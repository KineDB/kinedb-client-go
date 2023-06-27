package etcd

import (
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const serviceKeyPrefix = "/synapsedb/services/"

type EtcdClient struct {
	Cli         *clientv3.Client
	leaseID     clientv3.LeaseID
	ServiceInfo ServiceInfo
	Instance    string
}

var Client *EtcdClient

/**
* new etcd client
**/
func NewEtcdClient(endpoints []string, lease int64, dailTimeout int, serviceInfo ServiceInfo) (*EtcdClient, error) {
	if Client != nil {
		log.Infof("EtcdClient has already been initlized.")
		return Client, nil
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Duration(dailTimeout) * time.Second,
	})
	if err != nil {
		return nil, err
	}

	ec := &EtcdClient{
		Cli:         cli,
		ServiceInfo: serviceInfo,
		Instance:    serviceInfo.Name + "-" + serviceInfo.Host + ":" + strconv.Itoa(serviceInfo.Port),
	}

	log.Infof("Build NewEtcdClient success!")
	return ec, nil
}

func InitEtcdClient(serverConfig ServerConfig, serviceInfo ServiceInfo) *EtcdClient {
	// init etcd client
	var endpoints []string
	endpoints = append(endpoints, serverConfig.Addr)
	ec, err := NewEtcdClient(endpoints, 10, 60, serviceInfo)
	if err != nil {
		log.Errorf("InitEtcdClient failed err:%+v", err)
		return nil
	}
	Client = ec
	return ec
}
