package connector

import (
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

type Options struct {
}

type Connector struct {
	consumer sarama.Consumer
	hosts    []string
}

func NewConnector(hosts []string, options Options) *Connector {
	return &Connector{
		consumer: nil,
		hosts:    hosts,
	}
}

func (connector *Connector) Connect() error {
	log.WithFields(log.Fields{
		"hosts": connector.hosts,
	}).Info("Connecting to Kafka server")

	config := sarama.NewConfig()

	// Connect to kafka
	consumer, err := sarama.NewConsumer(connector.hosts, config)
	if err != nil {
		return err
	}

	connector.consumer = consumer

	return nil
}

func (connector *Connector) GetConsumer() sarama.Consumer {
	return connector.consumer
}
