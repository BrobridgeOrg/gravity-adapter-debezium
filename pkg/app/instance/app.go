package instance

import (
	"runtime"

	adapter_service "github.com/BrobridgeOrg/gravity-adapter-debezium/pkg/adapter/service"
	gravity_adapter "github.com/BrobridgeOrg/gravity-sdk/adapter"
	log "github.com/sirupsen/logrus"
)

type AppInstance struct {
	done             chan bool
	adapter          *adapter_service.Adapter
	adapterConnector *gravity_adapter.AdapterConnector
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

	// Initializing adapter connector
	err := a.initAdapterConnector()
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
