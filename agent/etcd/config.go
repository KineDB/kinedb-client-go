package etcd

import "github.com/industry-tenebris/kinedb-goclient/common/utils"

type ClientConfig struct {
	Ip      string
	BoundIp string
	Port    int
	Name    string
}

type ServerConfig struct {
	Addr string
}

func BoundIP(config *ClientConfig) {
	if len(config.Ip) == 0 {
		config.Ip = utils.GetOutboundIP()
		config.BoundIp = "0.0.0.0"
	} else {
		config.BoundIp = config.Ip
	}
}
