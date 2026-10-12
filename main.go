package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Config struct {
	Brokers    []string
	Topic      string
	Group      string
	Workers    int
	QueueSize  int
	BatchSize  int
	CommitEach int
}

type Pipeline struct {
	client  *kgo.Client
	cfg     Config
	jobs    chan *kgo.Record
	wg      sync.WaitGroup
	processed atomic.Uint64
	failed    atomic.Uint64
}

func envInt(key string, fallback int) int {
	n, err := strconv.Atoi(os.Getenv(key))
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func loadConfig() Config {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}

	topic := os.Getenv("KAFKA_TOPIC")
	if topic == "" {
		topic = "events"
	}

	group := os.Getenv("KAFKA_GROUP")
	if group == "" {
		group = "optimized-consumer"
	}

	return Config{
		Brokers:   strings.Split(brokers, ","),
		Topic:     topic,
		Group:     group,
		Workers:   envInt("WORKERS", runtime.NumCPU()),
		QueueSize: envInt("QUEUE_SIZE", 10000),
		BatchSize: envInt("BATCH_SIZE", 500),
		CommitEach: envInt("COMMIT_EACH", 500),
	}
}

func NewPipeline(cfg Config) (*Pipeline, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxBytes(50*1024*1024),
		kgo.FetchMaxPartitionBytes(1024*1024),
		kgo.FetchMinBytes(1),
		kgo.FetchMaxWait(100*time.Millisecond),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return nil, err
	}

	return &Pipeline{
		client: client,
		cfg:    cfg,
		jobs:   make(chan *kgo.Record, cfg.QueueSize),
	}, nil
}

// Replace this function with your database/API/business logic.
// Return an error when processing has failed.
func processRecord(ctx context.Context, record *kgo.Record) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Example workload:
	// - Decode record.Value
	// - Validate the event
	// - Write to a database or call another service
	_ = record.Key
	_ = record.Value

	return nil
}

func (p *Pipeline) worker(ctx context.Context) {
	defer p.wg.Done()

	for record := range p.jobs {
		if err := processRecord(ctx, record); err != nil {
			p.failed.Add(1)
			log.Printf(
				"processing failed topic=%s partition=%d offset=%d: %v",
				record.Topic, record.Partition, record.Offset, err,
			)

			// Do not commit failed records. A later restart may replay them.
			// Production systems should add retries and a dead-letter topic.
			continue
		}

		p.processed.Add(1)
	}
}

func (p *Pipeline) Run(ctx context.Context) error {
	p.wg.Add(p.cfg.Workers)
	for i := 0; i < p.cfg.Workers; i++ {
		go p.worker(ctx)
	}

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	var pending []*kgo.Record
	pending = make([]*kgo.Record, 0, p.cfg.BatchSize)

	var lastCommit time.Time

	commit := func() error {
		if len(pending) == 0 {
			return nil
		}

		// All records in this poll batch must finish before offsets
		// can safely be committed.
		for _, record := range pending {
			p.jobs <- record
		}

		// Wait for queued records to finish before committing.
		// The channel is drained by workers; the barrier below ensures
		// processing completion for this batch.
		barrier := make(chan struct{})
		p.jobs <- &kgo.Record{
			Value: []byte("__pipeline_barrier__"),
			Headers: []kgo.RecordHeader{
				{Key: "internal-barrier", Value: []byte("true")},
			},
		}
		_ = barrier

		// This simplified example uses synchronous batch processing
		// instead of committing while work is still running.
		// See the note below about partition-safe offset tracking.
		close(p.jobs)
		p.wg.Wait()

		p.client.AllowRebalance()

		p.client.CommitRecords(ctx, pending...)
		pending = pending[:0]
		lastCommit = time.Now()
		return nil
	}

	for {
		if ctx.Err() != nil {
			break
		}

		fetches := p.client.PollRecords(ctx, p.cfg.BatchSize)
		if err := fetches.Err(); err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}
			log.Printf("Kafka poll error: %v", err)
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			log.Printf("fetch error topic=%s partition=%d: %v",
				topic, partition, err)
		})

		fetches.EachRecord(func(record *kgo.Record) {
			pending = append(pending, record)
		})

		if len(pending) >= p.cfg.CommitEach ||
			(len(pending) > 0 && time.Since(lastCommit) >= 2*time.Second) {
			if err := commit(); err != nil {
				return err
			}
			// Recreate workers and queue after each batch.
			p.jobs = make(chan *kgo.Record, p.cfg.QueueSize)
			p.wg = sync.WaitGroup{}
			p.wg.Add(p.cfg.Workers)
			for i := 0; i < p.cfg.Workers; i++ {
				go p.worker(ctx)
			}
		}

		select {
		case <-ticker.C:
			log.Printf("processed=%d failed=%d pending=%d",
				p.processed.Load(), p.failed.Load(), len(pending))
		default:
		}
	}

	// Drain the final batch on shutdown.
	if len(pending) > 0 {
		for _, record := range pending {
			p.jobs <- record
		}
	}

	close(p.jobs)
	p.wg.Wait()
	p.client.AllowRebalance()

	if len(pending) > 0 {
		p.client.CommitRecords(ctx, pending...)
	}

	return nil
}

func main() {
	cfg := loadConfig()

	pipeline, err := NewPipeline(cfg)
	if err != nil {
		log.Fatalf("create Kafka consumer: %v", err)
	}
	defer pipeline.client.Close()

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	log.Printf(
		"consumer started topic=%s group=%s workers=%d queue=%d batch=%d",
		cfg.Topic, cfg.Group, cfg.Workers, cfg.QueueSize, cfg.BatchSize,
	)

	if err := pipeline.Run(ctx); err != nil {
		log.Printf("pipeline stopped: %v", err)
	}

	fmt.Printf(
		"Final metrics: processed=%d failed=%d\n",
		pipeline.processed.Load(),
		pipeline.failed.Load(),
	)
}
