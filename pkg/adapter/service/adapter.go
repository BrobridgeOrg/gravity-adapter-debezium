package adapter

import (
	"fmt"
	"os"
	"strings"

	"github.com/BrobridgeOrg/gravity-adapter-debezium/pkg/app"
	jsoniter "github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type Adapter struct {
	app        app.App
	sm         *SourceManager
	clientName string
}

func NewAdapter(a app.App) *Adapter {
	adapter := &Adapter{
		app: a,
	}

	adapter.sm = NewSourceManager(adapter)

	return adapter
}

func (adapter *Adapter) Init() error {

	// Using hostname (pod name) by default
	host, err := os.Hostname()
	if err != nil {
		log.Error(err)
		return err
	}

	host = strings.ReplaceAll(host, ".", "_")

	adapter.clientName = fmt.Sprintf("gravity_adapter_debezium-%s", host)

	err = adapter.sm.Initialize()
	if err != nil {
		log.Error(err)
		return err
	}

	return nil
}
