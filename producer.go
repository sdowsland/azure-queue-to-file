package main

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"time"

	"github.com/Azure/azure-storage-queue-go/2017-07-29/azqueue"
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

	// doneCount := 0
	// intervalCount := 100

	msgQueue := make(chan string)

	concurrentMsgProcessing := 5

	for n := 0; n < concurrentMsgProcessing; n++ {
		go (func(msgChan chan string) {
			for {
				msg := <-msgChan
				_, err = messagesURL.Enqueue(ctx, "This is message "+msg, time.Second*10, time.Hour*24*7)
				if err != nil {
					log.Fatal(err)
				}

			}
		})(msgQueue)
	}

	log.Println("Starting iter")

	for iter := 0; iter < viper.GetInt("producerCount"); iter++ {
		log.Println(strconv.Itoa(iter))
		msgQueue <- strconv.Itoa(iter)
	}
}
