package connector

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"

	"github.com/prometheus/common/log"
)

type InventoryConnector struct {
	Name   string                 `json:"name"`
	Config map[string]interface{} `json:"config"`
}

type Options struct {
	Name string
}

type Connector struct {
	host      string
	transport *http.Transport
	options   *Options
}

func NewConnector(host string, options Options) *Connector {
	return &Connector{
		host: host,
		transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		options: &options,
	}
}

func (connector *Connector) Register(config map[string]interface{}) error {

	ic := InventoryConnector{
		Name:   connector.options.Name,
		Config: config,
	}

	data, err := json.Marshal(&ic)
	if err != nil {
		return err
	}

	payload := bytes.NewReader(data)

	// Create a request
	request, err := http.NewRequest("POST", "http://"+connector.host+"/connectors/inventory-connector", payload)
	if err != nil {
		return err
	}

	// Preparing header
	request.Header.Add("Content-Type", "application/json")

	client := http.Client{
		Transport: connector.transport,
	}
	resp, err := client.Do(request)
	if err != nil {
		return err
	}

	// Require body
	defer resp.Body.Close()

	// Discard body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return errors.New("Failed to register inventory connector")
	}

	log.Info(string(body))

	return nil
}
