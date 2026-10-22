package client_test

import (
	"io"
	"net"
	"sort"
	"testing"
	"time"

	"gotunnel/internal/config"
	"gotunnel/internal/netutil"
)

// startSource runs a server that sends data as fast as the client reads it.
func startSource(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				buf := make([]byte, 32<<10)
				for {
					if _, err := c.Write(buf); err != nil {
						c.Close()
						return
					}
				}
			}()
		}
	}()
	return l.Addr().String()
}

// measureLatency downloads in bulk through one forward while timing 1-byte
// echoes through an interactive forward, over a slow link with deep buffers.
func measureLatency(t *testing.T, rate int, oneWay time.Duration, conn config.Connection) (median, p90 time.Duration, bulkRate float64) {
	fa := newFakeAgent(t)
	fa.pipe = func() (net.Conn, net.Conn) {
		sl := netutil.NewSlowLink(rate, oneWay)
		return sl.A, sl.B
	}
	echo, source := startEcho(t), startSource(t)
	interactive, bulk := freePort(t), freePort(t)
	cfg := baseConfig()
	cfg.Connection.ReplayBuffer = conn.ReplayBuffer
	cfg.Connection.MaxInflight = conn.MaxInflight
	cfg.Connection.AdaptiveInflight = conn.AdaptiveInflight
	cfg.Forwards = []config.Forward{
		{Name: "ssh", Listen: interactive, Target: echo, Mode: config.ModeAlways, Priority: config.PrioInteractive},
		{Name: "download", Listen: bulk, Target: source, Priority: config.PrioBulk},
	}
	startClient(t, cfg, fa)

	ic, err := net.Dial("tcp", interactive)
	if err != nil {
		t.Fatal(err)
	}
	defer ic.Close()
	ic.Write([]byte{'x'}) // open the stream before the download starts
	io.ReadFull(ic, make([]byte, 1))

	bc, err := net.Dial("tcp", bulk)
	if err != nil {
		t.Fatal(err)
	}
	defer bc.Close()
	downloaded := make(chan int64, 1)
	stop := make(chan struct{})
	bulkStart := time.Now()
	go func() {
		var n int64
		buf := make([]byte, 64<<10)
		for {
			select {
			case <-stop:
				downloaded <- n
				return
			default:
			}
			bc.SetReadDeadline(time.Now().Add(time.Second))
			k, _ := bc.Read(buf)
			n += int64(k)
		}
	}()

	time.Sleep(3 * time.Second) // let queues build up
	measureStart := time.Now()
	var samples []time.Duration
	for time.Since(measureStart) < 6*time.Second {
		start := time.Now()
		ic.SetDeadline(time.Now().Add(30 * time.Second))
		if _, err := ic.Write([]byte{'k'}); err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(ic, make([]byte, 1)); err != nil {
			t.Fatal(err)
		}
		samples = append(samples, time.Since(start))
		time.Sleep(100 * time.Millisecond)
	}
	close(stop)
	total := <-downloaded
	elapsed := time.Since(bulkStart).Seconds()
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	return samples[len(samples)/2], samples[len(samples)*9/10], float64(total) / elapsed / float64(rate)
}

func TestInteractiveLatencyDuringBulkTransfer(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	for _, link := range []struct {
		name      string
		rate      int // bytes per second
		oneWay    time.Duration
		maxMedian time.Duration
	}{
		{"1Mbps-rtt100ms", 128 << 10, 50 * time.Millisecond, 500 * time.Millisecond},
		{"256kbps-rtt300ms", 32 << 10, 150 * time.Millisecond, 2 * time.Second},
	} {
		t.Run(link.name, func(t *testing.T) {
			off := false
			fifo := config.Connection{ReplayBuffer: 8 << 20, MaxInflight: 8 << 20, AdaptiveInflight: &off}
			fifoMedian, fifoP90, fifoRate := measureLatency(t, link.rate, link.oneWay, fifo)
			t.Logf("without in-flight control: keystroke echo median %v, p90 %v; bulk uses %.0f%% of the link",
				fifoMedian.Round(time.Millisecond), fifoP90.Round(time.Millisecond), 100*fifoRate)

			adaptive := config.Connection{ReplayBuffer: 8 << 20, MaxInflight: 64 << 10}
			median, p90, rate := measureLatency(t, link.rate, link.oneWay, adaptive)
			t.Logf("with scheduler and adaptive in-flight limit: keystroke echo median %v, p90 %v; bulk uses %.0f%% of the link",
				median.Round(time.Millisecond), p90.Round(time.Millisecond), 100*rate)

			if median > link.maxMedian {
				t.Errorf("median keystroke latency %v exceeds %v", median, link.maxMedian)
			}
			if median*2 > fifoMedian {
				t.Errorf("scheduler did not clearly help: %v vs %v without it", median, fifoMedian)
			}
			if rate < 0.7 {
				t.Errorf("bulk transfer only used %.0f%% of the link", 100*rate)
			}
		})
	}
}
