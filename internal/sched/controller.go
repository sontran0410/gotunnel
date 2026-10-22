package sched

import (
	"sync"
	"time"
)

// Limiter is implemented by *session.Session.
type Limiter interface {
	SetSendLimit(n int)
}

// Controller tunes how many bytes may be in flight on the link.
//
// Too little in flight wastes bandwidth; too much builds a queue in ssh and
// network buffers that interactive traffic has to wait behind. The right
// amount is the path's bandwidth-delay product (BDP). The controller
// estimates both factors from acknowledged data, much like TCP BBR:
//
//   - bandwidth: the highest recent delivery rate,
//   - delay: the lowest recent round-trip time,
//
// and allows a little more than their product. While the estimated bandwidth
// keeps growing it allows twice the BDP to find the link's capacity quickly.
// Every 30s it briefly drains the queue so the minimum RTT can be measured
// again (a standing queue would otherwise hide it).
type Controller struct {
	mu       sync.Mutex
	target   Limiter
	adaptive bool
	min, max int
	headroom int
	limit    int
	now      func() time.Time

	minRTT   time.Duration
	minRTTAt time.Time

	bw         []bwSample
	roundStart time.Time
	roundBW    float64
	flatRounds int
	startup    bool

	probeUntil time.Time
	probeMin   time.Duration
}

type bwSample struct {
	at   time.Time
	rate float64
}

const (
	steadyGain       = 1.25
	startupGain      = 2.0
	probeRTTInterval = 30 * time.Second
)

// NewController returns a controller starting at initial bytes in flight and
// staying within [min, max]. headroom is added to the computed limit to cover
// acknowledgement granularity. If adaptive is false the limit stays fixed.
func NewController(initial, min, max, headroom int, adaptive bool) *Controller {
	return &Controller{
		adaptive: adaptive,
		min:      min,
		max:      max,
		headroom: headroom,
		limit:    clamp(initial, min, max),
		startup:  true,
		now:      time.Now,
	}
}

// Bind applies the controller to a session and sets its current limit.
func (c *Controller) Bind(t Limiter) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.target = t
	t.SetSendLimit(c.limit)
}

// Limit returns the current in-flight limit.
func (c *Controller) Limit() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.limit
}

// MinRTT returns the smallest recent round-trip time.
func (c *Controller) MinRTT() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.minRTT
}

// Observe feeds one measurement of an acknowledged frame: its RTT, the bytes
// in flight when it was sent and the delivery rate (bytes/s) meanwhile.
func (c *Controller) Observe(rtt time.Duration, inflight int, rate float64) {
	if !c.adaptive || rtt <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()

	// Minimum RTT, with periodic draining to re-measure it.
	switch {
	case !c.probeUntil.IsZero():
		if c.probeMin == 0 || rtt < c.probeMin {
			c.probeMin = rtt
		}
		if now.Before(c.probeUntil) {
			c.apply(c.min)
			return
		}
		c.minRTT, c.minRTTAt = c.probeMin, now
		c.probeUntil = time.Time{}
	case c.minRTT == 0 || rtt <= c.minRTT:
		c.minRTT, c.minRTTAt = rtt, now
	case now.Sub(c.minRTTAt) > probeRTTInterval:
		c.probeUntil = now.Add(max(200*time.Millisecond, 2*c.minRTT))
		c.probeMin = 0
		c.apply(c.min)
		return
	}

	// Bottleneck bandwidth: the maximum delivery rate over a sliding window.
	window := max(time.Second, 10*c.minRTT)
	c.bw = append(c.bw, bwSample{at: now, rate: rate})
	drop := 0
	for drop < len(c.bw)-1 && now.Sub(c.bw[drop].at) > window {
		drop++
	}
	c.bw = c.bw[drop:]
	var bw float64
	for _, s := range c.bw {
		bw = max(bw, s.rate)
	}

	// Once per round trip: keep the startup gain while bandwidth grows.
	if now.Sub(c.roundStart) >= c.minRTT {
		if bw >= c.roundBW*1.25 {
			c.startup, c.flatRounds = true, 0
		} else if c.flatRounds++; c.flatRounds >= 3 {
			c.startup = false
		}
		c.roundBW, c.roundStart = bw, now
	}
	gain := steadyGain
	if c.startup {
		gain = startupGain
	}
	c.apply(clamp(int(bw*c.minRTT.Seconds()*gain)+c.headroom, c.min, c.max))
}

func (c *Controller) apply(limit int) {
	if limit == c.limit {
		return
	}
	c.limit = limit
	if c.target != nil {
		c.target.SetSendLimit(limit)
	}
}

func clamp(v, lo, hi int) int { return max(lo, min(v, hi)) }
