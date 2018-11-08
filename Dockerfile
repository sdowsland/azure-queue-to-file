FROM golang

RUN go get github.com/Azure/azure-storage-queue-go/2017-07-29/azqueue
RUN go get github.com/spf13/viper
