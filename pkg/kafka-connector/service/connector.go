package connector

import (
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

type Options struct {
	ClientName string
}

type Connector struct {
	options Options
	group   sarama.ConsumerGroup
	hosts   []string
}

func NewConnector(hosts []string, options Options) *Connector {
	return &Connector{
		hosts:   hosts,
		options: options,
	}
}

func (connector *Connector) Connect() error {

	log.WithFields(log.Fields{
		"hosts": connector.hosts,
	}).Info("Connecting to Kafka server")

	config := sarama.NewConfig()

	// Connect to kafka
	group, err := sarama.NewConsumerGroup(connector.hosts, connector.options.ClientName, config)
	if err != nil {
		return err
	}

	connector.group = group

	return nil
}

func (connector *Connector) GetConsumerGroup() sarama.ConsumerGroup {
	return connector.group
}
