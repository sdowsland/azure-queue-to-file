package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/sdowsland/azure-storage-queue-go/2017-07-29/azqueue"
	"github.com/spf13/viper"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {

	viper.SetConfigName("config")         // name of config file (without extension)
	viper.AddConfigPath("$HOME/.appname") // call multiple times to add many search paths
	viper.AddConfigPath(".")              // optionally look for config in the working directory
	err := viper.ReadInConfig()           // Find and read the config file
	if err != nil {                       // Handle errors reading the config file
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	accountName := viper.GetString("accountName")

	credentials := azqueue.NewSharedKeyCredential(accountName, viper.GetString("accountKey"))

	p := azqueue.NewPipeline(credentials, azqueue.PipelineOptions{})

	u, _ := url.Parse(fmt.Sprintf("http://%s.queue.core.windows.net", accountName))

	serviceURL := azqueue.NewServiceURL(*u, p)

	queueURL := serviceURL.NewQueueURL(viper.GetString("queueName"))

	messagesURL := queueURL.NewMessagesURL()

	ctx := context.TODO()

	const concurrentMsgProcessing = 16 // Set this as you desire
	msgCh := make(chan *azqueue.DequeuedMessage, concurrentMsgProcessing)
	const poisonMessageDequeueThreshold = 4

	writeChannel := make(chan string, concurrentMsgProcessing)
	doneWriting := make(chan string)

	doneCount := 0
	intervalCount := 100

	go func(channel chan string) {
		currentTime := time.Now().Local()

		f, err := os.Create("output/" + viper.GetString("queueName") + "-" + currentTime.Format("2006-01-02T15:04:05") + ".json")
		check(err)

		w := bufio.NewWriter(f)

		for {
			j, more := <-channel

			if more {
				w.WriteString(j + "\n")

				err = w.Flush() // Don't forget to flush!
				if err != nil {
					log.Fatal(err)
				}

				doneCount = doneCount + 1

				if doneCount%intervalCount == 0 {
					fmt.Println(doneCount)
				}
			} else {
				f.Close()
				doneWriting <- "done"
			}
		}

	}(writeChannel)

	// Create goroutines that can process messages in parallel
	for n := 0; n < concurrentMsgProcessing; n++ {
		go func(msgCh <-chan *azqueue.DequeuedMessage) {
			for {
				msg := <-msgCh // Get a message from the channel

				// Create a URL allowing you to manipulate this message.
				// This returns a MessageIDURL object that wraps the this message's URL and a request pipeline (inherited from messagesURL)
				msgIDURL := messagesURL.NewMessageIDURL(msg.ID)
				popReceipt := msg.PopReceipt // This message's most-recent pop receipt

				if msg.DequeueCount > poisonMessageDequeueThreshold {
					// This message has attempted to be processed too many times; treat it as a poison message
					// DO NOT attempt to process this message
					// Log this message as a poison message somewhere (code not shown)
					// Delete this poison message from the queue so it will never be dequeued again
					msgIDURL.Delete(ctx, popReceipt)
					continue // Process a different message
				}

				// This message is not a poison message, process it (this example just displays it):
				// fmt.Print(msg.Text + "\n")
				writeChannel <- msg.Text

				// NOTE: You can examine/use any of the message's other properties as you desire:
				_, _, _ = msg.InsertionTime, msg.ExpirationTime, msg.NextVisibleTime

				// OPTIONAL: while processing a message, you can update the message's visibility timeout
				// (to prevent other servers from dequeuing the same message simultaneously) and update the
				// message's text (to prevent some successfully-completed processing from re-executing the
				// next time this message is dequeued):
				// update, err := msgIDURL.Update(ctx, popReceipt, time.Second*20, "updated msg")
				// if err != nil {
				// 	log.Fatal(err)
				// }
				// popReceipt = update.PopReceipt // Performing any operation on a message ID always requires the most recent pop receipt

				// After processing the message, delete it from the queue so it won't be dequeued ever again:
				_, err = msgIDURL.Delete(ctx, popReceipt)
				if err != nil {
					log.Fatal(err)
				}
				// Loop around to process the next message
			}
		}(msgCh)
	}

	// The code below shows the service's infinite loop that dequeues messages and dispatches them in batches for processsing:
	for {
		// Try to dequeue a batch of messages from the queue
		// dequeue, err := messagesURL.Dequeue(ctx, azqueue.QueueMaxMessagesDequeue, 10*time.Second)
		dequeue, err := messagesURL.Dequeue(ctx, azqueue.QueueMaxMessagesDequeue, 10*time.Second)
		if err != nil {
			log.Fatal(err)
		}
		if dequeue.NumMessages() == 0 {
			// The queue was empty; sleep a bit and try again
			// Shorter time means higher costs & less latency to dequeue a message
			// Higher time means lower costs & more latency to dequeue a message
			time.Sleep(time.Second * 10)
		} else {
			// We got some messages, put them in the channel so that many can be processed in parallel:
			// NOTE: The queue does not guarantee FIFO ordering & processing messages in parallel also does
			// not preserve FIFO ordering. So, the "Output:" order below is not guaranteed but usually works.
			fmt.Println(dequeue.NumMessages())
			fmt.Println(" messages retrieved.")
			for m := int32(0); m < dequeue.NumMessages(); m++ {
				msgCh <- dequeue.Message(m)
				// time.Sleep(time.Second * 100)
			}
		}
		// This batch of dequeued messages are in the channel, dequeue another batch
		// break // NOTE: For this example only, break out of the infinite loop so this example terminates
	}

}
