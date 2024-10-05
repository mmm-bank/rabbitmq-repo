package messaging

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

var _ Publisher = RabbitMQPublisher{}
var _ Consumer = RabbitMQConsumer{}

type Publisher interface {
	PublishMessage(queueName string, message []byte) error
}

type Consumer interface {
	ConsumeMessages(queueName string, handlerFunc func(message []byte) error) error
}

type RabbitMQPublisher struct {
	conn *amqp.Connection
}

type RabbitMQConsumer struct {
	conn *amqp.Connection
}

func CreateConn(rabbitMQAddr string) *amqp.Connection {
	conn, err := amqp.Dial(rabbitMQAddr)
	if err != nil {
		log.Fatalf("failed to connect to RabbitMQ: %v", err)
	}
	return conn
}

func DeclareQueue(queueName string, conn *amqp.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	return err
}

func NewPublisher(conn *amqp.Connection) RabbitMQPublisher {
	return RabbitMQPublisher{conn}
}

func NewConsumer(conn *amqp.Connection) RabbitMQConsumer {
	return RabbitMQConsumer{conn}
}

func (p RabbitMQPublisher) PublishMessage(queueName string, message []byte) error {
	channel, err := p.conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer channel.Close()

	err = channel.Publish(
		"",
		queueName,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        message,
		})
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

func (c RabbitMQConsumer) ConsumeMessages(queueName string, handlerFunc func(message []byte) error) error {
	channel, err := c.conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer channel.Close()

	msgs, err := channel.Consume(
		queueName,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to consume messages: %w", err)
	}

	for msg := range msgs {
		err := handlerFunc(msg.Body)
		if err != nil {
			fmt.Printf("failed to handle message: %v", err)
		}
	}

	return nil
}
