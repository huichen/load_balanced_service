package load_balanced_service

import (
	"errors"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

var ErrEmptyService = errors.New("empty service")

type LoadBalancedService struct {
	etcdClient client.Client
	connected  bool
	nodes      []string
	nodeMap    map[string]int
	nodeCount  int
	sync.RWMutex
}

func (service *LoadBalancedService) watch(watcher client.Watcher) {
	for {
		resp, err := watcher.Next(context.Background())
		if err == nil {
			if resp.Action == "set" {
				n := resp.Node.Value
				service.addNode(n)
			} else if resp.Action == "delete" {
				n := resp.PrevNode.Value
				service.removeNode(n)
			}
		}
	}
}

// serviceName is like "/services/busybox"
// endPoints is an array of "http://<etcd client ip:port>"
func (service *LoadBalancedService) Connect(serviceName string, endPoints []string) error {
	if service.connected {
		log.Printf("Can't connected twice")
		return errors.New("math: square root of negative number")
	}

	service.nodeMap = make(map[string]int)
	service.nodeCount = 0

	cfg := client.Config{
		Endpoints:               endPoints,
		Transport:               client.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second,
	}

	var err error
	service.etcdClient, err = client.New(cfg)
	if err != nil {
		return err
	}
	kapi := client.NewKeysAPI(service.etcdClient)

	resp, err := kapi.Get(context.Background(), serviceName, nil)
	if err != nil {
		return err
	} else {
		if resp.Node.Dir {
			for _, peer := range resp.Node.Nodes {
				n := peer.Value
				service.addNode(n)
			}
		}
	}

	watcher := kapi.Watcher(serviceName, &client.WatcherOptions{Recursive: true})
	go service.watch(watcher)
	service.connected = true
	return nil
}

func (service *LoadBalancedService) GetNode() (string, error) {
	if !service.connected {
		return "", errors.New("Must call connect first")
	}

	service.RLock()
	defer service.RUnlock()

	if service.nodeCount == 0 {
		return "", ErrEmptyService
	}

	return service.nodes[rand.Intn(service.nodeCount)], nil
}

func (service *LoadBalancedService) addNode(key string) {
	service.Lock()
	defer service.Unlock()

	if _, ok := service.nodeMap[key]; !ok {
		if service.nodeCount >= len(service.nodes) {
			service.nodes = append(service.nodes, key)
		} else {
			service.nodes[service.nodeCount] = key
		}
		service.nodeMap[key] = service.nodeCount
		service.nodeCount++
	}
}

func (service *LoadBalancedService) removeNode(key string) {
	service.Lock()
	defer service.Unlock()

	if index, ok := service.nodeMap[key]; ok {
		for i := index; i < len(service.nodes)-1; i++ {
			service.nodes[i] = service.nodes[i+1]
		}
		service.nodeCount--
		delete(service.nodeMap, key)
	}
}
