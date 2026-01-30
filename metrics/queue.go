package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	namespaceQueue = "queue"
	queueName      = "name"
	queueType      = "type" // main, retry, dlq
)

var (
	MetricQueueEnqueueTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceQueue,
		Name:      "queue_enqueue_total",
		Help:      "Total number of actual data pushed to queue.",
	}, []string{queueName})

	MetricQueueEnqueueErrorTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceQueue,
		Name:      "queue_enqueue_error_total",
		Help:      "Total number of actual data pushed to queue failed.",
	}, []string{queueName})

	MetricQueueDequeueTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceQueue,
		Name:      "queue_dequeue_total",
		Help:      "Total number of actual data poped from queue.",
	}, []string{queueName})

	MetricQueueDequeueErrorTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceQueue,
		Name:      "queue_dequeue_error_total",
		Help:      "Total number of actual data poped from queue failed.",
	}, []string{queueName})

	MetricQueueHandleErrorTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceQueue,
		Name:      "queue_handle_error_total",
		Help:      "Total number of actual data for handling error",
	}, []string{queueName})

	MetricQueueHandleSuccessulTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceQueue,
		Name:      "queue_handle_successful_total",
		Help:      "Total number of actual data for handling successful",
	}, []string{queueName})

	MetricQueueRecoverTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceQueue,
		Name:      "queue_recover_total",
		Help:      "Total number of actual data for recovering",
	}, []string{queueName})

	MetricQueueRecoverSuccessulTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceQueue,
		Name:      "queue_recover_successful_total",
		Help:      "Total number of actual data for recovering successful",
	}, []string{queueName})

	MetricQueueRecoverErrorTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceQueue,
		Name:      "queue_recover_error_total",
		Help:      "Total number of actual data for recovering failed",
	}, []string{queueName})

	MetricQueuePurgeTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceQueue,
		Name:      "queue_purge_total",
		Help:      "Total number of actual data for purging",
	}, []string{queueName})

	MetricQueuePurgeSuccessulTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceQueue,
		Name:      "queue_purge_successful_total",
		Help:      "Total number of actual data for purging successful",
	}, []string{queueName})

	MetricQueuePurgeErrorTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceQueue,
		Name:      "queue_purge_error_total",
		Help:      "Total number of actual data for purging failed",
	}, []string{queueName})

	MetricQueueHandleDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespaceQueue,
		Name:      "queue_handle_duration_seconds",
		Help:      "Duration of handling for a queue.",
		Buckets: []float64{
			0.001, // 1 ms
			0.005, // 5 ms
			0.01,  // 10 ms
			0.025, // 25 ms
			0.05,  // 50 ms
			0.1,   // 100 ms
			0.25,  // 250 ms
			0.5,   // 500 ms
			1,     // 1 s
			2.5,   // 2.5 s
			5,     // 5 s
			10,    // 10 s
			20,    // 20s
			30,    // 30 s
			60,    // 60 s
			300,   // 5 min
		},
	}, []string{queueName})

	// ==================== Gauge Metrics ====================

	// MetricQueueDepth tracks the current number of messages in the queue.
	// Labels: name (queue name), type (main, retry, dlq)
	MetricQueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceQueue,
		Name:      "queue_depth",
		Help:      "Current number of messages in the queue.",
	}, []string{queueName, queueType})

	// MetricQueueInflight tracks the number of messages currently being processed.
	MetricQueueInflight = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceQueue,
		Name:      "queue_inflight",
		Help:      "Number of messages currently being processed (in-flight).",
	}, []string{queueName})

	// MetricQueueConsumersActive tracks the number of active consumers/subscribers.
	MetricQueueConsumersActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceQueue,
		Name:      "queue_consumers_active",
		Help:      "Number of active consumers (callbacks) registered for the queue.",
	}, []string{queueName})

	// MetricQueueCapacity tracks the maximum capacity of the queue.
	// Value is -1 for unlimited queues.
	MetricQueueCapacity = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceQueue,
		Name:      "queue_capacity",
		Help:      "Maximum capacity of the queue (-1 for unlimited).",
	}, []string{queueName, queueType})

	// ==================== Counter Metrics (New) ====================

	// MetricQueueDLQMessagesTotal tracks the total number of messages sent to DLQ.
	MetricQueueDLQMessagesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceQueue,
		Name:      "queue_dlq_messages_total",
		Help:      "Total number of messages sent to Dead Letter Queue.",
	}, []string{queueName})

	// MetricQueueRedriveTotal tracks the total number of DLQ redrive operations.
	MetricQueueRedriveTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceQueue,
		Name:      "queue_redrive_total",
		Help:      "Total number of DLQ redrive operations.",
	}, []string{queueName})

	// MetricQueueRedriveSuccessfulTotal tracks successful redrive operations.
	MetricQueueRedriveSuccessfulTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceQueue,
		Name:      "queue_redrive_successful_total",
		Help:      "Total number of successful DLQ redrive operations.",
	}, []string{queueName})

	// MetricQueueRedriveErrorTotal tracks failed redrive operations.
	MetricQueueRedriveErrorTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceQueue,
		Name:      "queue_redrive_error_total",
		Help:      "Total number of failed DLQ redrive operations.",
	}, []string{queueName})

	// ==================== Histogram Metrics (New) ====================

	// MetricQueueMessageAge tracks the age of messages when dequeued (time since creation).
	MetricQueueMessageAge = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespaceQueue,
		Name:      "queue_message_age_seconds",
		Help:      "Age of messages when dequeued (time since creation).",
		Buckets: []float64{
			0.001,  // 1 ms
			0.01,   // 10 ms
			0.1,    // 100 ms
			0.5,    // 500 ms
			1,      // 1 s
			5,      // 5 s
			10,     // 10 s
			30,     // 30 s
			60,     // 1 min
			300,    // 5 min
			600,    // 10 min
			1800,   // 30 min
			3600,   // 1 hour
			86400,  // 1 day
		},
	}, []string{queueName})

	// MetricQueueEnqueueDuration tracks the duration of enqueue operations.
	MetricQueueEnqueueDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespaceQueue,
		Name:      "queue_enqueue_duration_seconds",
		Help:      "Duration of enqueue operations.",
		Buckets: []float64{
			0.00001, // 10 µs
			0.00005, // 50 µs
			0.0001,  // 100 µs
			0.0005,  // 500 µs
			0.001,   // 1 ms
			0.005,   // 5 ms
			0.01,    // 10 ms
			0.05,    // 50 ms
			0.1,     // 100 ms
			0.5,     // 500 ms
			1,       // 1 s
		},
	}, []string{queueName})

	// MetricQueueDequeueDuration tracks the duration of dequeue operations.
	MetricQueueDequeueDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespaceQueue,
		Name:      "queue_dequeue_duration_seconds",
		Help:      "Duration of dequeue operations.",
		Buckets: []float64{
			0.00001, // 10 µs
			0.00005, // 50 µs
			0.0001,  // 100 µs
			0.0005,  // 500 µs
			0.001,   // 1 ms
			0.005,   // 5 ms
			0.01,    // 10 ms
			0.05,    // 50 ms
			0.1,     // 100 ms
			0.5,     // 500 ms
			1,       // 1 s
		},
	}, []string{queueName})

	// ==================== Info Metric ====================

	// MetricQueueConfigInfo exposes queue configuration as labels.
	// Value is always 1, configuration is exposed via labels.
	MetricQueueConfigInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceQueue,
		Name:      "queue_config_info",
		Help:      "Queue configuration information (value is always 1).",
	}, []string{queueName, "max_size", "max_handle_failures", "consumer_count", "callback_parallel"})
)
