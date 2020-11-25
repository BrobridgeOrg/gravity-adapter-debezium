package adapter

import (
	"context"
	"strings"
	"sync"
	"unsafe"

	debezium_connector "github.com/BrobridgeOrg/gravity-adapter-debezium/pkg/debezium-connector/service"
	kafka_connector "github.com/BrobridgeOrg/gravity-adapter-debezium/pkg/kafka-connector/service"
	dsa "github.com/BrobridgeOrg/gravity-api/service/dsa"
	parallel_chunked_flow "github.com/cfsghost/parallel-chunked-flow"
	jsoniter "github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
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
	incoming          chan []byte
	name              string
	host              string
	kafkaHosts        []string
	tables            []string
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

	info := sourceInfo

	// Initialize parapllel chunked flow
	pcfOpts := parallel_chunked_flow.Options{
		BufferSize: 204800,
		ChunkSize:  512,
		ChunkCount: 512,
		Handler: func(data interface{}, output chan interface{}) {
			/*
				id := atomic.AddUint64((*uint64)(&counter), 1)
				if id%1000 == 0 {
					log.Info(id)
				}
			*/
			eventName := jsoniter.Get(data.([]byte), "event").ToString()
			payload := jsoniter.Get(data.([]byte), "payload").ToString()

			// Preparing request
			request := requestPool.Get().(*dsa.PublishRequest)
			request.EventName = eventName
			request.Payload = StrToBytes(payload)

			output <- request
		},
	}

	return &Source{
		adapter:    adapter,
		incoming:   make(chan []byte, 204800),
		name:       name,
		host:       info.Host,
		kafkaHosts: strings.Split(info.KafkaHosts, ","),
		configs:    info.Configs,
		tables:     info.Tables,
		parser:     parallel_chunked_flow.NewParallelChunkedFlow(&pcfOpts),
	}
}

func (source *Source) InitSubscription() error {

	log.WithFields(log.Fields{
		"source":      source.name,
		"client_name": source.adapter.clientName + "-" + source.name,
	}).Info("Initializing subscriber ...")

	databaseServerName := source.configs["database.server.name"].(string)

	// Preparing topics
	topics := make([]string, 0, len(source.tables))
	for _, tableName := range source.tables {
		topics = append(topics, databaseServerName+".public."+tableName)
	}

	group := source.kafkaConnector.GetConsumerGroup()

	// Setup consumer
	go func() {
		consumer := NewConsumer()

		for {
			ctx := context.Background()
			err := group.Consume(ctx, topics, consumer)
			if err != nil {
				log.Error(err)
			}

			if ctx.Err() != nil {
				log.Error("Kafka Consumer timeout")
			}
		}
	}()

	return source.InitDebezium()
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

	// Initializing gRPC streams
	p := source.adapter.app.GetGRPCPool()

	// Register initializer for stream
	p.SetStreamInitializer("publish", func(conn *grpc.ClientConn) (interface{}, error) {
		client := dsa.NewDataSourceAdapterClient(conn)
		return client.PublishEvents(context.Background())
	})

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

	go source.eventReceiver()
	go source.requestHandler()

	return source.InitSubscription()
}

func (source *Source) eventReceiver() {

	log.WithFields(log.Fields{
		"source":      source.name,
		"client_name": source.adapter.clientName + "-" + source.name,
	}).Info("Initializing event receiver ...")

	for {
		select {
		case msg := <-source.incoming:
			source.parser.Push(msg)
		}
	}
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

	// Getting stream from pool
	err := source.adapter.app.GetGRPCPool().GetStream("publish", func(s interface{}) error {

		// Send request
		return s.(dsa.DataSourceAdapter_PublishEventsClient).Send(request)
	})
	if err != nil {
		log.Error("Failed to get available stream:", err)
		return
	}
}
