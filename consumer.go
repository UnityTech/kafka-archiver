package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/glog"
)

func (w *Worker) consume() {
	defer func() {
		if err := w.consumer.Close(); err != nil {
			glog.Warning("could not release resources for worker, this could result in a leak: ", err)
		}
		w.closeBuffers()
		if err := metrics.Close(); err != nil {
			glog.Warning("could not closes handles to metrics, this could result in a leak: ", err)
		}
		w.wg.Done()
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	// Consumer delay takes care of delaying a consumer by a certain amount of seconds
	// until the consumer is ready to start.
	// This is extremely useful in cases where new consumers start at or around the same time
	// causing the group to converge and archiver to forcefully flush all the files.
	// To overcome this issue, a user can set the -consumer-delay-start flag.
	if w.consumerDelayStart.Seconds() > 0 {
		glog.Infof("consumer delay set to %v", w.consumerDelayStart)

	Delay:
		for {
			select {
			case <-time.After(w.consumerDelayStart.Duration):
				glog.Info("breaking consumer delay")
				break Delay
			case <-w.consumer.Notifications():
				glog.Info("received rebalance notification, resetting consumer delay")
			}
		}
	}

	for {
		select {
		case msg, ok := <-w.consumer.Messages():
			// Listen on each new message.
			if !ok {
				continue
			}
			start := time.Now()
			// Forward the message to the handler.
			glog.V(8).Infof("received message: %+v", string(msg.Value))
			if err := w.handleMessage(msg); err != nil {
				glog.Error(err)
			}
			metrics.Timing("request", time.Since(start), nil, 1)
		case err := <-w.consumer.Errors():
			// Listen on consumer errors.
			metrics.Incr("error", []string{"kind:consumer"}, 1)
			glog.Errorf("consumer error: %v", err)
		case <-signals:
			// Listens on system signals.
			glog.Info("received system signal")
			return
		case n := <-w.consumer.Notifications():
			// Listen on consumer re-balance notifications.
			metrics.Incr("rebalance", nil, 1)
			glog.Info("received rebalance notification")
			w.partitioner.Rebalance(n)
		}
	}
}

// handleMessage takes care of handling a consumer message. Messages may be delivered
// more than once if an error is thrown.
func (w *Worker) handleMessage(msg *sarama.ConsumerMessage) error {
	buf, err := w.partitioner.GetBuffer(msg)
	if err != nil {
		return err
	}

	_, err = buf.WriteMessage(msg)
	return err
}
