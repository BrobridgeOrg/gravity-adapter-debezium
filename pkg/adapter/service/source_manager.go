package adapter

import (
	"io/ioutil"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type SourceConfig struct {
	Sources map[string]SourceInfo `json:"sources"`
}

type SourceInfo struct {
	Host       string                 `json:"host"`
	KafkaHosts string                 `json:"kafka.hosts"`
	Tables     map[string]SourceTable `json:"tables"`
	Configs    map[string]interface{} `json:"configs"`
}

type SourceTable struct {
	Events SourceTableEvents `json:"events"`
}

type SourceTableEvents struct {
	Snapshot string `json:"snapshot"`
	Create   string `json:"create"`
	Update   string `json:"update"`
	Delete   string `json:"delete"`
}

type SourceManager struct {
	adapter *Adapter
	sources map[string]*Source
}

func NewSourceManager(adapter *Adapter) *SourceManager {
	return &SourceManager{
		adapter: adapter,
		sources: make(map[string]*Source),
	}
}

func (sm *SourceManager) Initialize() error {

	config, err := sm.LoadSourceConfig(viper.GetString("source.config"))
	if err != nil {
		return err
	}

	// Initializing sources
	for name, info := range config.Sources {

		log.WithFields(log.Fields{
			"name":  name,
			"hosts": info.KafkaHosts,
		}).Info("Initializing source")

		source := NewSource(sm.adapter, name, &info)
		err := source.Init()
		if err != nil {
			log.Error(err)
			return err
		}

		sm.sources[name] = source
	}

	return nil
}

func (sm *SourceManager) LoadSourceConfig(filename string) (*SourceConfig, error) {

	// Open configuration file
	jsonFile, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer jsonFile.Close()

	// Read
	byteValue, _ := ioutil.ReadAll(jsonFile)

	var config SourceConfig

	json.Unmarshal(byteValue, &config)

	return &config, nil
}
