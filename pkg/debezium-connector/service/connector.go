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

func (connector *Connector) GetConfigs() (*InventoryConnector, error) {

	// Create a request
	request, err := http.NewRequest("GET", "http://"+connector.host+"/connectors/"+connector.options.Name, nil)
	if err != nil {
		return nil, err
	}

	// Preparing header
	request.Header.Add("Content-Type", "application/json")

	client := http.Client{
		Transport: connector.transport,
	}
	resp, err := client.Do(request)
	if err != nil {
		return nil, err
	}

	// Require body
	defer resp.Body.Close()

	// Discard body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Not found
	if resp.StatusCode == 404 {
		return nil, nil
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, errors.New("Failed to get configs of inventory connector")
	}

	var ic InventoryConnector
	err = json.Unmarshal(body, &ic)
	if err != nil {
		return nil, err
	}

	return &ic, nil
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
	request, err := http.NewRequest("POST", "http://"+connector.host+"/connectors/", payload)
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

func (connector *Connector) Delete() error {

	// Create a request
	request, err := http.NewRequest("DELETE", "http://"+connector.host+"/connectors/"+connector.options.Name, nil)
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
		log.Info(string(body))
		return errors.New("Failed to delete inventory connector")
	}

	return nil
}
