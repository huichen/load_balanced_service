package main

import (
	"flag"
	"github.com/huichen/load_balanced_service"
	"log"
	"strings"
	"time"
)

var (
	endPoints   = flag.String("endpoints", "", "Comma-separated endpoints of your etcd cluster, each starting with http://.")
	serviceName = flag.String("service_name", "", "Name of your service in etcd.")
)

func main() {
	flag.Parse()

	ep := strings.Split(*endPoints, ",")
	if len(ep) == 0 {
		log.Fatal("Can't parse --endpoints")
	}

	if *serviceName == "" {
		log.Fatal("--service_name can't be empty")
	}

	var service load_balanced_service.LoadBalancedService
	err := service.Connect(*serviceName, ep)
	if err != nil {
		log.Fatal(err)
	}

	for {
		node, _ := service.GetNode()
		if node != "" {
			log.Printf("assigned to node: %v", node)
		} else {
			log.Printf("no assignment")
		}
		time.Sleep(time.Second)
	}
}
