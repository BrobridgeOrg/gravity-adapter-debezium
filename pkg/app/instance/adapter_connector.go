package instance

import (
	gravity_adapter "github.com/BrobridgeOrg/gravity-sdk/adapter"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func (a *AppInstance) initAdapterConnector() error {

	host := viper.GetString("dsa.host")

	log.WithFields(log.Fields{
		"host": host,
	}).Info("Initializing adapter connector")

	// Initializing gravity adapter connector
	a.adapterConnector = gravity_adapter.NewAdapterConnector()
	opts := gravity_adapter.NewOptions()
	err := a.adapterConnector.Connect(host, opts)
	if err != nil {
		return err
	}

	return nil
}

func (a *AppInstance) GetAdapterConnector() *gravity_adapter.AdapterConnector {
	return a.adapterConnector
}
