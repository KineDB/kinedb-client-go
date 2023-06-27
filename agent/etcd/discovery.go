package etcd

import (
	"context"
	"sync"
	"time"

	"github.com/KineDB/kinedb-client-go/common/utils"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ServiceDiscovery struct
type ServiceDiscovery struct {
	ec         *EtcdClient             //etcd client
	serverList map[string]*ServiceInfo //service list
	lock       sync.Mutex
}

var serviceDiscovery *ServiceDiscovery

func FindServiceByDb(host string, port int, schema string) *ServiceInfo {
	for _, service := range serviceDiscovery.serverList {
		if service.ServiceType == Engine && service.DbHost == host && service.DbPort == port && service.DbSchema == schema {
			return service
		}
	}
	return nil
}

func FindServiceByType(serviceType ServiceType) []*ServiceInfo {
	var findServiceList []*ServiceInfo
	for _, service := range serviceDiscovery.serverList {
		if service.ServiceType == serviceType {
			findServiceList = append(findServiceList, service)
		}
	}
	return findServiceList
}

func FindAllService() []*ServiceInfo {
	var findServiceList []*ServiceInfo
	for _, service := range serviceDiscovery.serverList {
		findServiceList = append(findServiceList, service)
	}
	return findServiceList
}

type ServiceType string

const (
	Agent      ServiceType = "Agent"
	Dispatcher ServiceType = "Dispatcher"
	PreEngine  ServiceType = "PreEngine"
	Engine     ServiceType = "Engine"
	SMeta      ServiceType = "SMeta"
	SManager   ServiceType = "SManager"
	Synapse    ServiceType = "Synapse"
	SLog       ServiceType = "SLog"
)

type ServiceInfo struct {
	Name string
	Host string
	Port int

	ServiceType ServiceType
	DbHost      string
	DbPort      int
	DbSchema    string
}

// prefix like /mysql_engine /redis_engine
func (s *ServiceDiscovery) WatchService(prefix string) error {
	resp, err := s.ec.Cli.Get(context.Background(), prefix, clientv3.WithPrefix())
	log.Infof("WatchService prefix:%s resp:%+v", prefix, resp)
	if err != nil {
		return err
	}

	for _, ev := range resp.Kvs {
		s.PutService(string(ev.Key), string(ev.Value))
	}

	go s.watcher(prefix)
	return nil
}

func (s *ServiceDiscovery) watcher(prefix string) {
	rch := s.ec.Cli.Watch(context.Background(), prefix, clientv3.WithPrefix())
	log.Infof("watching prefix:%s now...", prefix)
	for wresp := range rch {
		for _, ev := range wresp.Events {
			switch ev.Type {
			case mvccpb.PUT:
				s.PutService(string(ev.Kv.Key), string(ev.Kv.Value))
			case mvccpb.DELETE:
				s.DelService(string(ev.Kv.Key))
			}
		}
	}
}

func (s *ServiceDiscovery) PutService(key string, value string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	var serviceInfo ServiceInfo
	utils.JsonString2Struct(value, &serviceInfo)
	s.serverList[key] = &serviceInfo
	log.Infof("PutService key:[%s] value:[%+v]", key, value)
}

func (s *ServiceDiscovery) DelService(key string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	delete(s.serverList, key)
	log.Infof("DelService key:[%s]", key)
}

func discoveryService(prefix string) {
	serviceDiscovery = &ServiceDiscovery{
		ec:         Client,
		serverList: make(map[string]*ServiceInfo),
	}
	go serviceDiscovery.watchService(prefix)
}

func DiscoveryServiceSync(prefix string) *ServiceDiscovery {
	serviceDiscovery = &ServiceDiscovery{
		ec:         Client,
		serverList: make(map[string]*ServiceInfo),
	}
	serviceDiscovery.WatchService(prefix)
	return serviceDiscovery
}

func (s *ServiceDiscovery) watchService(prefix string) {

	s.WatchService(prefix) // watch prefix /engines
	for range time.Tick(180 * time.Second) {
		log.Infof("serviceList: %+v", s.serverList)
	}
}
