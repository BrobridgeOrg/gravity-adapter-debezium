package adapter

import (
	"sync"

	"github.com/Shopify/sarama"
)

type ConsumerEvent struct {
	Session sarama.ConsumerGroupSession
	Message *sarama.ConsumerMessage
}

var consumerEventPool = sync.Pool{
	New: func() interface{} {
		return &ConsumerEvent{}
	},
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	source *Source
	ready  chan bool
}

func NewConsumer(source *Source) *Consumer {
	return &Consumer{
		source: source,
		ready:  make(chan bool),
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for message := range claim.Messages() {

		if len(message.Value) == 0 {
			session.MarkMessage(message, "")
			continue
		}

		event := consumerEventPool.Get().(*ConsumerEvent)
		event.Session = session
		event.Message = message

		consumer.source.parser.Push(event)
	}

	return nil
}
