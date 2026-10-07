package main

import (
	"context"
	"log"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
)

type BatchWriter struct {
	client *influxdb3.Client
	in     chan *influxdb3.Point
	size   int
	every  time.Duration
	done   chan struct{}
}

func NewBatchWriter(c *influxdb3.Client, size int, every time.Duration) *BatchWriter {
	return &BatchWriter{
		client: c,
		in:     make(chan *influxdb3.Point, size*4),
		size:   size,
		every:  every,
		done:   make(chan struct{}),
	}
}

func (w *BatchWriter) Add(p *influxdb3.Point) { w.in <- p }

func (w *BatchWriter) Run(ctx context.Context) {
	defer close(w.done)
	ticker := time.NewTicker(w.every)
	defer ticker.Stop()
	buf := make([]*influxdb3.Point, 0, w.size)

	flush := func() {
		if len(buf) == 0 {
			return
		}
		wctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := w.client.WritePoints(wctx, buf); err != nil {
			log.Printf("write failed (%d points): %v", len(buf), err)
		} else {
			log.Printf("wrote %d points", len(buf))
		}
		buf = buf[:0]
	}

	for {
		select {
		case p := <-w.in:
			buf = append(buf, p)
			if len(buf) >= w.size {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			for { // drain whatever is left in the channel before exiting
				select {
				case p := <-w.in:
					buf = append(buf, p)
				default:
					flush()
					return
				}
			}
		}
	}
}

// Wait blocks until Run has done its final flush.
func (w *BatchWriter) Wait() { <-w.done }
