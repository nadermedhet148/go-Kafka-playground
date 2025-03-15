package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/Shopify/sarama"
	"github.com/gofiber/fiber/v2"
)

// Comment struct
type Comment struct {
	Text string `form:"text" json:"text"`
}

func main() {

	app := fiber.New()
	api := app.Group("/api/v1") // /api

	api.Post("/comments", createComment)

	app.Listen(":3000")

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

func PushCommentToQueue(topic string, message []byte) error {

	brokersUrl := []string{"localhost:29092"}
	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		return err
	}

	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)

	return nil
}

func PushCommentToQueueWithPartition(topic string, message []byte, partition int32) error {

	brokersUrl := []string{"localhost:29092"}
	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		return err
	}

	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Value:     sarama.StringEncoder(message),
		Partition: partition,
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)

	return nil
}

// createComment handler
func createComment(c *fiber.Ctx) error {

	// Instantiate new Message struct
	cmt, cmtInBytes, err, shouldReturn, err := getCommentBytes(c)
	if shouldReturn {
		return err
	}
	PushCommentToQueue("comments", cmtInBytes)

	// Return Comment in JSON format
	return handleResponse(err, c, cmt)
}

// createComment handler
func createCommentWithPartition(c *fiber.Ctx) error {

	// Instantiate new Message struct
	cmt, cmtInBytes, err, shouldReturn, err := getCommentBytes(c)
	if shouldReturn {
		return err
	}
	PushCommentToQueueWithPartition("comments", cmtInBytes)

	// Return Comment in JSON format
	return handleResponse(err, c, cmt)
}

func handleResponse(err error, c *fiber.Ctx, cmt *Comment) error {
	err = c.JSON(&fiber.Map{
		"success": true,
		"message": "Comment pushed successfully",
		"comment": cmt,
	})
	if err != nil {
		c.Status(500).JSON(&fiber.Map{
			"success": false,
			"message": "Error creating product",
		})
		return err
	}

	return err
}

func getCommentBytes(c *fiber.Ctx) (*Comment, []byte, error, bool, error) {
	cmt := new(Comment)

	//  Parse body into comment struct
	if err := c.BodyParser(cmt); err != nil {
		log.Println(err)
		c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": err,
		})
		return nil, nil, nil,

			// convert body into bytes and send it to kafka
			true, err
	}

	cmtInBytes, err := json.Marshal(cmt)
	return cmt, cmtInBytes, err, false, nil
}
