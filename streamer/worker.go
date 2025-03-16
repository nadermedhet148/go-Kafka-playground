package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/bxcodec/faker/v3"
	"github.com/gmbyapa/kstream/v2/kafka"
	"github.com/gmbyapa/kstream/v2/kafka/adaptors/librd"
	"github.com/gmbyapa/kstream/v2/streams"
	"github.com/gmbyapa/kstream/v2/streams/encoding"
	"github.com/tryfix/log"
)

var bootstrapServers = flag.String(`bootstrap-servers`, `localhost:29092`,
	`A comma seperated list Kafka Bootstrap Servers`)

const TopicTextLines = `textlines`
const TopicWordCount = `word-counts`

func main() {
	flag.Parse()

	config := streams.NewStreamBuilderConfig()
	config.BootstrapServers = strings.Split(*bootstrapServers, `,`)
	config.ApplicationId = `word-count`
	config.Consumer.Offsets.Initial = kafka.OffsetEarliest

	seed(config.Logger)

	builder := streams.NewStreamBuilder(config)
	buildTopology(config.Logger, builder)

	topology, err := builder.Build()
	if err != nil {
		panic(err)
	}

	println("Topology - \n", topology.Describe())

	runner := builder.NewRunner()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)

	go func() {
		<-sigs
		if err := runner.Stop(); err != nil {
			println(err)
		}
	}()

	if err := runner.Run(topology); err != nil {
		panic(err)
	}
}

func buildTopology(logger log.Logger, builder *streams.StreamBuilder) {
	streams.WithReplicaCount(1)
	stream := builder.KStream(TopicTextLines, encoding.StringEncoder{}, encoding.StringEncoder{})
	stream.Each(func(ctx context.Context, key, value interface{}) {
		logger.Debug(`Word count for : ` + value.(string))
	}).FlatMapValues(func(ctx context.Context, key, value interface{}) (values []interface{}, err error) {
		for _, word := range strings.Split(value.(string), ` `) {
			values = append(values, word)
		}
		return
	}).SelectKey(func(ctx context.Context, key, value interface{}) (kOut interface{}, err error) {
		return value, nil
	}).Repartition(`textlines-by-word`).Aggregate(`word-count`,
		func(ctx context.Context, key, value, previous interface{}) (newAgg interface{}, err error) {
			var count int
			if previous != nil {
				count = previous.(int)
			}
			count++
			newAgg = count

			return
		}, streams.AggregateWithValEncoder(encoding.IntEncoder{})).ToStream().Each(func(ctx context.Context, key, value interface{}) {
		println(fmt.Sprintf(`%s:%d`, key, value))
	}).To(TopicWordCount)
}

func seed(logger log.Logger) {
	conf := librd.NewProducerConfig()
	conf.BootstrapServers = strings.Split(*bootstrapServers, `,`)
	conf.Transactional.Enabled = false
	conf.Transactional.Id = `words-producer`
	for i := 0; i < 100; i++ {
		brokersUrl := []string{"localhost:29092"}
		producer, err := ConnectProducer(brokersUrl)
		if err != nil {
			logger.Error("Failed to connect producer: ", err)
			continue
		}

		defer producer.Close()

		msg := &sarama.ProducerMessage{
			Topic: TopicTextLines,
			Value: sarama.StringEncoder(faker.Sentence()),
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			logger.Error("Failed to send message: ", err)
			continue
		}

		fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", TopicTextLines, partition, offset)
		logger.Debug("Message produced to topic: " + TopicTextLines)
	}

	logger.Info("Test records produced")
	logger.Info(`Test records produced`)
}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
