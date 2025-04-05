package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	namespaceQueue = "queue"
	queueName      = "name"
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

	MetricQueueHandleDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespaceQueue,
		Name:      "queue_handle_duration_seconds",
		Help:      "Duration of handling for a queue.",
		Buckets: []float64{
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
)
