package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"

	"github.com/golang/glog"
)

var (
	// Version indicates the current version of the Kafka Achiever.
	Version = "0.100"
)

func init() {
	// Always log to stderr.
	flag.Set("logtostderr", "true")

	// Start pprof server.
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
}

func main() {
	w := NewWorker()
	w.Init()
	glog.Info("exiting")
}
