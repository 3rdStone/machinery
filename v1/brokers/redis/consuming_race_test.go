package redis_test

import (
	"net"
	"testing"
	"time"

	"github.com/3rdStone/machinery/v1/brokers/errs"
	"github.com/3rdStone/machinery/v1/brokers/iface"
	"github.com/3rdStone/machinery/v1/brokers/redis"
	"github.com/3rdStone/machinery/v1/config"
)

// closedAddr returns a local address on which nothing is listening, so every
// connection attempt fails immediately with "connection refused".
func closedAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func testConfig() *config.Config {
	return &config.Config{
		DefaultQueue: "machinery_race_test",
		Redis:        &config.RedisConfig{ConnectTimeout: 1, ReadTimeout: 1, WriteTimeout: 1},
	}
}

func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not return in time", what)
	}
}

// startStopRace launches StartConsuming in a goroutine and immediately calls
// StopConsuming, mimicking Worker.LaunchAsync followed by Worker.Quit. Before
// the fix, consumingWG.Add(1) in StartConsuming could run concurrently with
// consumingWG.Wait() in StopConsuming, which the race detector reports.
func startStopRace(t *testing.T, iterations int, newBroker func() iface.Broker) {
	for i := 0; i < iterations; i++ {
		b := newBroker()

		started := make(chan struct{})
		go func() {
			defer close(started)
			retry, err := b.StartConsuming("race_test", 1, nil)
			if err == errs.ErrConsumerStopped && retry {
				t.Errorf("StartConsuming returned ErrConsumerStopped with retry=true")
			}
		}()

		stopped := make(chan struct{})
		go func() {
			defer close(stopped)
			b.StopConsuming()
		}()

		waitClosed(t, stopped, "StopConsuming")
		waitClosed(t, started, "StartConsuming")
	}
}

// stopThenStart checks that StartConsuming called after StopConsuming does not
// start consuming and tells the worker loop not to retry.
func stopThenStart(t *testing.T, b iface.Broker) {
	b.StopConsuming()
	retry, err := b.StartConsuming("race_test", 1, nil)
	if retry {
		t.Errorf("expected retry=false after StopConsuming, got true")
	}
	if err != errs.ErrConsumerStopped {
		t.Errorf("expected ErrConsumerStopped, got %v", err)
	}
}

func TestBrokerStartStopConsumingRace(t *testing.T) {
	addr := closedAddr(t)
	startStopRace(t, 200, func() iface.Broker {
		return redis.New(testConfig(), addr, "", "", 0)
	})
}

func TestBrokerStartConsumingAfterStop(t *testing.T) {
	stopThenStart(t, redis.New(testConfig(), closedAddr(t), "", "", 0))
}

func TestBrokerGRStartStopConsumingRace(t *testing.T) {
	addr := closedAddr(t)
	// go-redis retries a failed dial with backoff, so keep this loop shorter.
	startStopRace(t, 20, func() iface.Broker {
		return redis.NewGR(testConfig(), []string{addr}, 0)
	})
}

func TestBrokerGRStartConsumingAfterStop(t *testing.T) {
	stopThenStart(t, redis.NewGR(testConfig(), []string{closedAddr(t)}, 0))
}
