package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	namespaceState         = "state"
	stateStorageType       = "storage_type"
	stateStorageOperation  = "storage_operation"
	stateSnapshotOperation = "snapshot_operation"
)

type StateStorageOperation string

const (
	StateStorageOperationLoadState      StateStorageOperation = "LoadState"
	StateStorageOperationLoadAllStates  StateStorageOperation = "LoadAllStates"
	StateStorageOperationSaveStates     StateStorageOperation = "SaveStates"
	StateStorageOperationClearStates    StateStorageOperation = "ClearStates"
	StateStorageOperationClearAllStates StateStorageOperation = "ClearAllStates"
	StateStorageOperationGetStateIDs    StateStorageOperation = "GetStateIDs"
	StateStorageOperationGetStateNames  StateStorageOperation = "GetStateNames"
)

type StateSnapshotOperation string

const (
	StateSnapshotOperationSnapshotStates         StateSnapshotOperation = "SnapshotStates"
	StateSnapshotOperationRevertStatesToSnapshot StateSnapshotOperation = "RevertStatesToSnapshot"
	StateSnapshotOperationGetSnapshot            StateSnapshotOperation = "GetSnapshot"
	StateSnapshotOperationDeleteSnapshot         StateSnapshotOperation = "DeleteSnapshot"
	StateSnapshotOperationClearSnapshots         StateSnapshotOperation = "ClearSnapshots"
)

var (
	MetricStateStorageOperationTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceState,
		Name:      "storage_operation_total",
		Help:      "Total number of state storage operations.",
	}, []string{stateStorageType, stateStorageOperation})

	MetricStateStorageOperationErrorTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceState,
		Name:      "storage_operation_error_total",
		Help:      "Total number of failures of state storage operations.",
	}, []string{stateStorageType, stateStorageOperation})

	MetricStateStorageOperationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespaceQueue,
		Name:      "storage_operation_duration_seconds",
		Help:      "Duration of handling for a storage operation.",
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
	}, []string{stateStorageType, stateStorageOperation})

	MetricStateSnapshotOperationTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceState,
		Name:      "snapshot_operation_total",
		Help:      "Total number of state snapshot operations.",
	}, []string{stateStorageType, stateSnapshotOperation})

	MetricStateSnapshotOperationErrorTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespaceState,
		Name:      "snapshot_operation_error_total",
		Help:      "Total number of failures of state snapshot operations.",
	}, []string{stateStorageType, stateSnapshotOperation})

	MetricStateSnapshotOperationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespaceQueue,
		Name:      "snapshot_operation_duration_seconds",
		Help:      "Duration of handling for a snapshot operation.",
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
	}, []string{stateStorageType, stateSnapshotOperation})
)
