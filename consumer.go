package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"time"
)

func main() {
	wg := new(sync.WaitGroup)
	var subscribedTopics []string
	config := cluster.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.CommitInterval = 1 * time.Millisecond
	config.Group.Return.Notifications = true
	MessagesChan := make(chan *sarama.ConsumerMessage)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	brokers := "kafka01:9092,kafka02:9092,kafka03:9092"
	client, _ := cluster.NewClient(strings.Split(brokers, ","), config)
	allTopics, _ := client.Topics()
	defer client.Close()
	for _, topic := range allTopics {
		if strings.HasPrefix(topic, "topicprefix-") {
			subscribedTopics = append(subscribedTopics, topic)
		}
	}
	fmt.Println(subscribedTopics)
	consumer, _ := cluster.NewConsumer(strings.Split(brokers, ","), "graphite-cg", subscribedTopics, config)
	defer consumer.Close()

	for i := 0; i < 1; i++ {
		wg.Add(2)
		go Start(MessagesChan, consumer)
		go readAndPublish(consumer, MessagesChan)
	}
	wg.Wait()

}

func readAndPublish(consumer *cluster.Consumer, MessagesChan chan *sarama.ConsumerMessage) {
	defer fmt.Println("quitting")
	for {
		msg := <-consumer.Messages()
		if msg == nil {
			continue
		} else {
			MessagesChan <- msg
		}
	}

}
func Start(messages chan *sarama.ConsumerMessage, consumer *cluster.Consumer) {
	var conn net.Conn
	conn = dialTCP("127.0.0.1:2003", 10)
	for {
		select {
		case msg := <-messages:
			if msg == nil {
				fmt.Println("msg is null")
				continue
			}
			metricList := strings.Split(string(msg.Value), "|")
			metricName := metricList[0]
			for i := 1; i < len(metricList); i += 2 {

				x := metricName + " " + metricList[i+1] + " " + metricList[i] + "\n"
				_, err := conn.Write([]byte(x))
				if err == nil {
					consumer.MarkPartitionOffset(msg.Topic, msg.Partition, msg.Offset, "")
				} else {
					conn.Close()
					conn = dialTCP("127.0.0.1:2003", 10)
					i = 1
					fmt.Println("Failed to write " + metricName)
				}
			}
		}
	}
}

func dialTCP(ipAndPort string, timeout time.Duration) net.Conn {
	var conn net.Conn
	for i := 1; i <= 10; i++ {
		var DialError error
		conn, DialError = net.Dial("tcp", ipAndPort)
		if DialError != nil {
			fmt.Println(DialError)
			time.Sleep(time.Second * 5)
			timeout += 1
		} else {
			break
		}
	}
	return conn
}
