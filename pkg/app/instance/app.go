package instance

import (
	"runtime"

	adapter_service "github.com/BrobridgeOrg/gravity-adapter-debezium/pkg/adapter/service"
	grpc_connection_pool "github.com/cfsghost/grpc-connection-pool"
	log "github.com/sirupsen/logrus"
)

type AppInstance struct {
	done     chan bool
	grpcPool *grpc_connection_pool.GRPCPool
	adapter  *adapter_service.Adapter
}

func NewAppInstance() *AppInstance {

	a := &AppInstance{
		done: make(chan bool),
	}

	a.adapter = adapter_service.NewAdapter(a)

	return a
}

func (a *AppInstance) Init() error {

	log.WithFields(log.Fields{
		"max_procs": runtime.GOMAXPROCS(0),
	}).Info("Starting application")

	// Initializing gRPC pool
	err := a.initGRPCPool()
	if err != nil {
		return err
	}

	err = a.adapter.Init()
	if err != nil {
		return err
	}

	return nil
}

func (a *AppInstance) Uninit() {
}

func (a *AppInstance) Run() error {

	<-a.done

	return nil
}
