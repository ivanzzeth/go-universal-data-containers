package pubsub

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics contains all Prometheus metrics for a PubSub instance
type Metrics struct {
	// PublishTotal counts total publish operations per topic
	PublishTotal *prometheus.CounterVec

	// DeliveredTotal counts total messages delivered to subscribers per topic
	DeliveredTotal *prometheus.CounterVec

	// DroppedTotal counts messages dropped due to full buffers per topic
	DroppedTotal *prometheus.CounterVec

	// SubscribeTotal counts total subscribe operations per topic
	SubscribeTotal *prometheus.CounterVec

	// UnsubscribeTotal counts total unsubscribe operations per topic
	UnsubscribeTotal *prometheus.CounterVec

	// SubscribersGauge tracks current number of subscribers per topic
	SubscribersGauge *prometheus.GaugeVec

	// HandlerErrorTotal counts handler errors per topic
	HandlerErrorTotal *prometheus.CounterVec

	// PublishLatency tracks publish operation latency per topic
	PublishLatency *prometheus.HistogramVec

	// BatchSize tracks batch sizes for PublishBatch operations
	BatchSize *prometheus.HistogramVec

	// PipelineLatency tracks pipeline execution latency
	PipelineLatency *prometheus.HistogramVec

	// PipelineErrorTotal counts pipeline execution errors
	PipelineErrorTotal *prometheus.CounterVec
}

var (
	// Singleton metrics instances per backend
	metricsRegistry = make(map[string]*Metrics)
	metricsOnce     sync.Map // map[string]*sync.Once
	metricsMu       sync.Mutex
)

// NewMetrics creates or returns the existing Metrics instance for the given backend.
// Metrics are registered only once per backend to avoid duplicate registration panics.
func NewMetrics(backend string) *Metrics {
	// Get or create sync.Once for this backend
	onceVal, _ := metricsOnce.LoadOrStore(backend, &sync.Once{})
	once := onceVal.(*sync.Once)

	once.Do(func() {
		metricsMu.Lock()
		defer metricsMu.Unlock()

		labels := []string{"topic"}

		m := &Metrics{
			PublishTotal: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Namespace:   "pubsub",
					Subsystem:   backend,
					Name:        "publish_total",
					Help:        "Total number of publish operations",
					ConstLabels: prometheus.Labels{"backend": backend},
				},
				labels,
			),

			DeliveredTotal: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Namespace:   "pubsub",
					Subsystem:   backend,
					Name:        "delivered_total",
					Help:        "Total number of messages delivered to subscribers",
					ConstLabels: prometheus.Labels{"backend": backend},
				},
				labels,
			),

			DroppedTotal: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Namespace:   "pubsub",
					Subsystem:   backend,
					Name:        "dropped_total",
					Help:        "Total number of messages dropped due to full buffers",
					ConstLabels: prometheus.Labels{"backend": backend},
				},
				labels,
			),

			SubscribeTotal: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Namespace:   "pubsub",
					Subsystem:   backend,
					Name:        "subscribe_total",
					Help:        "Total number of subscribe operations",
					ConstLabels: prometheus.Labels{"backend": backend},
				},
				labels,
			),

			UnsubscribeTotal: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Namespace:   "pubsub",
					Subsystem:   backend,
					Name:        "unsubscribe_total",
					Help:        "Total number of unsubscribe operations",
					ConstLabels: prometheus.Labels{"backend": backend},
				},
				labels,
			),

			SubscribersGauge: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Namespace:   "pubsub",
					Subsystem:   backend,
					Name:        "subscribers",
					Help:        "Current number of subscribers per topic",
					ConstLabels: prometheus.Labels{"backend": backend},
				},
				labels,
			),

			HandlerErrorTotal: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Namespace:   "pubsub",
					Subsystem:   backend,
					Name:        "handler_error_total",
					Help:        "Total number of handler errors",
					ConstLabels: prometheus.Labels{"backend": backend},
				},
				labels,
			),

			PublishLatency: prometheus.NewHistogramVec(
				prometheus.HistogramOpts{
					Namespace:   "pubsub",
					Subsystem:   backend,
					Name:        "publish_latency_seconds",
					Help:        "Publish operation latency in seconds",
					Buckets:     prometheus.DefBuckets,
					ConstLabels: prometheus.Labels{"backend": backend},
				},
				labels,
			),

			BatchSize: prometheus.NewHistogramVec(
				prometheus.HistogramOpts{
					Namespace:   "pubsub",
					Subsystem:   backend,
					Name:        "batch_size",
					Help:        "Batch size for PublishBatch operations",
					Buckets:     []float64{1, 10, 50, 100, 250, 500, 1000, 2500, 5000, 10000},
					ConstLabels: prometheus.Labels{"backend": backend},
				},
				labels,
			),

			PipelineLatency: prometheus.NewHistogramVec(
				prometheus.HistogramOpts{
					Namespace:   "pubsub",
					Subsystem:   backend,
					Name:        "pipeline_latency_seconds",
					Help:        "Pipeline execution latency in seconds",
					Buckets:     []float64{.0001, .0005, .001, .005, .01, .025, .05, .1, .25, .5, 1},
					ConstLabels: prometheus.Labels{"backend": backend},
				},
				labels,
			),

			PipelineErrorTotal: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Namespace:   "pubsub",
					Subsystem:   backend,
					Name:        "pipeline_error_total",
					Help:        "Total number of pipeline execution errors",
					ConstLabels: prometheus.Labels{"backend": backend},
				},
				labels,
			),
		}

		// Register all metrics (ignore errors for already registered metrics)
		prometheus.DefaultRegisterer.Register(m.PublishTotal)
		prometheus.DefaultRegisterer.Register(m.DeliveredTotal)
		prometheus.DefaultRegisterer.Register(m.DroppedTotal)
		prometheus.DefaultRegisterer.Register(m.SubscribeTotal)
		prometheus.DefaultRegisterer.Register(m.UnsubscribeTotal)
		prometheus.DefaultRegisterer.Register(m.SubscribersGauge)
		prometheus.DefaultRegisterer.Register(m.HandlerErrorTotal)
		prometheus.DefaultRegisterer.Register(m.PublishLatency)
		prometheus.DefaultRegisterer.Register(m.BatchSize)
		prometheus.DefaultRegisterer.Register(m.PipelineLatency)
		prometheus.DefaultRegisterer.Register(m.PipelineErrorTotal)

		metricsRegistry[backend] = m
	})

	metricsMu.Lock()
	defer metricsMu.Unlock()
	return metricsRegistry[backend]
}
