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
	
	payload := "{\"gpuSerial\":\"0324917052143\",\"driverVersion\":396.26,\"@version\":\"1\",\"powerLimitWatt\":250,\"powerMaxLimitWatt\":250,\"throttleReasonsActive\":\"0x0000000000000001\",\"throttleReasonsAppsClocksSetting\":\"Not Active\",\"gpuCoreIndex\":0,\"throttleReasonsPowerCap\":\"Not Active\",\"hostname\":\"4a79b6d2616049edbf06c6aa58ab426a00000R\",\"powerDrawWatt\":25.75,\"timestamp\":\"2018-11-08T06:28:52.228Z\",\"gpuMemUtilPerc\":0,\"gpuCoreCount\":1,\"gpuPerformanceState\":\"P0\",\"gpuUtilPerc\":0,\"path\":\"/home/ubuntu/gpu.log\",\"message\":\"2018/11/08 06:28:52.228, 0 %, 11 MiB, 16160 MiB, 0 %, 28, 25.75 W, 250.00 W, 250.00 W, 0324917052143, GPU-c63d0fbe-70ed-615f-0204-36eb091de996, 396.26, Tesla V100-PCIE-16GB, 1, 0, P0, 0x00000000000000FF, 0x0000000000000001, Active, Not Active, Not Active, Not Active, Not Active, 4a79b6d2616049edbf06c6aa58ab426a00000R\",\"gpuMemTotalMeg\":16160,\"gpuName\":\"Tesla V100-PCIE-16GB\",\"throttleReasonsGpuIdle\":\"Active\",\"gpuUuid\":\"GPU-c63d0fbe-70ed-615f-0204-36eb091de996\",\"@timestamp\":\"2018-11-08T06:28:52.228Z\",\"gpuMemUsedMeg\":11,\"throttleReasonsSupported\":\"0x00000000000000FF\",\"throttleReasonsSlowdown\":\"Not Active\",\"throttleReasonsSyncBoost\":\"Not Active\",\"host\":\"4a79b6d2616049edbf06c6aa58ab426a00000R\",\"gpuTempC\":28}"

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

	doneCount := 0
	intervalCount := 1000

	msgQueue := make(chan string)

	concurrentMsgProcessing := 5

	for n := 0; n < concurrentMsgProcessing; n++ {
		go (func(msgChan chan string) {
			for {
				msg := <-msgChan
				_, err = messagesURL.Enqueue(ctx, payload, time.Second*10, time.Hour*24*7)
				if err != nil {
					log.PrintLn(err)
					time.Sleep(time.Second * 10)
					msgChan <- msg
				}

			}
		})(msgQueue)
	}

	log.Println("Starting iter")

	for iter := 0; iter < viper.GetInt("producerCount"); iter++ {
// 		log.Println(strconv.Itoa(iter))
		doneCount = doneCount + 1
                if doneCount%intervalCount == 0 {
		  log.Println(doneCount)
                }
		msgQueue <- strconv.Itoa(iter)
	}
}
