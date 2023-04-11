package kinedb

import "github.com/industry-tenebris/kinedb-goclient/agent/etcd"

func DiscoveryAddr(etcdAddr string) {
	config := etcd.ServerConfig{
		Addr: etcdAddr,
	}
	serviceInfo := etcd.ServiceInfo{
		Name:        "go-client",
		Host:        etcdAddr,
		ServiceType: etcd.Synapse,
	}
	etcd.InitEtcdClient(config, serviceInfo)
	etcd.DiscoveryServiceSync("/synapsedb/services/")
}
