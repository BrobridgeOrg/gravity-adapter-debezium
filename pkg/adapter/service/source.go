package adapter

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
	"unsafe"

	debezium_connector "github.com/BrobridgeOrg/gravity-adapter-debezium/pkg/debezium-connector/service"
	kafka_connector "github.com/BrobridgeOrg/gravity-adapter-debezium/pkg/kafka-connector/service"
	dsa "github.com/BrobridgeOrg/gravity-api/service/dsa"
	parallel_chunked_flow "github.com/cfsghost/parallel-chunked-flow"
	log "github.com/sirupsen/logrus"
)

var counter uint64

// Default settings
var DefaultWorkerCount int = 128
var DefaultPingInterval int64 = 10
var DefaultMaxPingsOutstanding int = 3
var DefaultMaxReconnects int = -1

type Packet struct {
	EventName string      `json:"event"`
	Payload   interface{} `json:"payload"`
}

type Source struct {
	adapter           *Adapter
	kafkaConnector    *kafka_connector.Connector
	debeziumConnector *debezium_connector.Connector
	name              string
	host              string
	kafkaHosts        []string
	schema            string
	tables            map[string]SourceTable
	configs           map[string]interface{}
	parser            *parallel_chunked_flow.ParallelChunkedFlow
}

var requestPool = sync.Pool{
	New: func() interface{} {
		return &dsa.PublishRequest{}
	},
}

func StrToBytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}

func NewSource(adapter *Adapter, name string, sourceInfo *SourceInfo) *Source {

	if len(sourceInfo.Host) == 0 {
		log.WithFields(log.Fields{
			"source": name,
		}).Error("Required host")

		return nil
	}

	// required kafka.hosts
	if len(sourceInfo.KafkaHosts) == 0 {
		log.WithFields(log.Fields{
			"source": name,
		}).Error("Required kafka.hosts")

		return nil
	}

	_, ok := sourceInfo.Configs["database.server.name"]
	if !ok {
		log.WithFields(log.Fields{
			"source": name,
		}).Error("Required database.server.name in configs")

		return nil
	}

	connClass := sourceInfo.Configs["connector.class"]
	schema := ""
	switch {
	case connClass == "io.debezium.connector.oracle.OracleConnector":
		schema = fmt.Sprintf("%v", sourceInfo.Configs["database.schema"])
	case connClass == "io.debezium.connector.postgresql.PostgresConnector":
		schema = "public"
	}

	info := sourceInfo

	source := &Source{
		adapter:    adapter,
		name:       name,
		host:       info.Host,
		kafkaHosts: strings.Split(info.KafkaHosts, ","),
		configs:    info.Configs,
		schema:     schema,
		tables:     info.Tables,
	}

	// Initialize parapllel chunked flow
	pcfOpts := parallel_chunked_flow.Options{
		BufferSize: 204800,
		ChunkSize:  512,
		ChunkCount: 512,
		Handler: func(data interface{}, output func(interface{})) {
			/*
				id := atomic.AddUint64((*uint64)(&counter), 1)
				if id%1000 == 0 {
					log.Info(id)
				}
			*/

			ce := data.(*ConsumerEvent)

			source.parseEvent(ce, output)
		},
	}

	source.parser = parallel_chunked_flow.NewParallelChunkedFlow(&pcfOpts)

	return source
}

func (source *Source) InitSubscription() error {

	err := source.InitDebezium()
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"source":      source.name,
		"client_name": source.adapter.clientName + "-" + source.name,
	}).Info("Initializing subscriber ...")

	databaseServerName := source.configs["database.server.name"].(string)

	// Preparing topics
	topics := make([]string, 0, len(source.tables))
	for tableName, _ := range source.tables {
		topics = append(topics, databaseServerName+"."+source.schema+"."+tableName)
	}

	group := source.kafkaConnector.GetConsumerGroup()

	// Setup consumer
	go func() {
		consumer := NewConsumer(source)
		ctx := context.Background()
		for {
			err := group.Consume(ctx, topics, consumer)
			if err != nil {
				log.Error(err)
			}

			if ctx.Err() != nil {
				log.Error("Kafka Consumer timeout")
			}
		}
	}()

	return nil
}

func (source *Source) InitDebezium() error {

	log.WithFields(log.Fields{
		"connector": source.name,
		"class":     source.configs["connector.class"].(string),
	}).Info("Checking inventory connector")
	ic, err := source.debeziumConnector.GetConfigs()
	if err != nil {
		return err
	}

	if ic != nil {
		log.Info("Replacing inventory connector with new configs")
		err := source.debeziumConnector.Delete()
		if err != nil {
			return err
		}
	} else {
		log.Info("Registering inventory connector to debezium")
	}

	return source.debeziumConnector.Register(source.configs)
}

func (source *Source) Init() error {

	// Connect to kafka for retriving events from debezium
	log.WithFields(log.Fields{
		"source": source.name,
		"hosts":  source.kafkaHosts,
	}).Info("Initializing kafka connector")

	options := kafka_connector.Options{
		ClientName: source.adapter.clientName + "-" + source.name,
	}
	c := kafka_connector.NewConnector(source.kafkaHosts, options)
	err := c.Connect()
	if err != nil {
		return err
	}

	source.kafkaConnector = c

	// Initializing debezium connector
	log.WithFields(log.Fields{
		"source": source.name,
		"hosts":  source.host,
	}).Info("Initializing debezium connector")

	opts := debezium_connector.Options{
		Name: source.name,
	}

	dc := debezium_connector.NewConnector(source.host, opts)

	source.debeziumConnector = dc

	go source.requestHandler()

	return source.InitSubscription()
}

func (source *Source) parseEvent(ce *ConsumerEvent, output func(interface{})) {

	defer consumerEventPool.Put(ce)
	defer ce.Session.MarkMessage(ce.Message, "")

	// Prepare event
	event := eventPool.Get().(*Event)
	defer eventPool.Put(event)

	// Parsing event
	err := json.Unmarshal(ce.Message.Value, event)
	if err != nil {
		log.Error(string(ce.Message.Value))
		log.Error(err)
		return
	}

	// Getting table information
	tableInfo, ok := source.tables[event.Payload.Source.Table]

	if !ok {
		return
	}

	// Preparing request
	request := requestPool.Get().(*dsa.PublishRequest)

	switch event.Payload.Op {
	case "c":
		payload, err := json.Marshal(event.Payload.After)
		if err != nil {
			return
		}

		request.Payload = payload

		if event.Payload.Source.IsSnapshot() {
			request.EventName = tableInfo.Events.Snapshot
		} else {
			request.EventName = tableInfo.Events.Create
		}
	case "u":
		payload, err := json.Marshal(event.Payload.After)
		if err != nil {
			return
		}

		request.Payload = payload
		request.EventName = tableInfo.Events.Update
	case "d":

		data := make(map[string]interface{})
		for key, value := range event.Payload.Before {

			if value == nil {
				continue
			}

			data[key] = value
		}

		payload, err := json.Marshal(data)
		if err != nil {
			return
		}

		request.EventName = tableInfo.Events.Delete
		request.Payload = payload
	case "r":
		payload, err := json.Marshal(event.Payload.After)
		if err != nil {
			return
		}

		request.Payload = payload
		request.EventName = tableInfo.Events.Create
	}

	//check
	if request.EventName == "" {
		return
	}

	output(request)
}

func (source *Source) requestHandler() {

	log.WithFields(log.Fields{
		"source":      source.name,
		"client_name": source.adapter.clientName + "-" + source.name,
	}).Info("Initializing request handler...")

	for {
		select {
		case req := <-source.parser.Output():
			source.HandleRequest(req.(*dsa.PublishRequest))
			requestPool.Put(req)
		}
	}
}

func (source *Source) HandleRequest(request *dsa.PublishRequest) {

	for {
		connector := source.adapter.app.GetAdapterConnector()
		err := connector.Publish(request.EventName, request.Payload, nil)
		if err != nil {
			log.Error(err)
			time.Sleep(time.Second)
			continue
		}

		break
	}
}
