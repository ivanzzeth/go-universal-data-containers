package state

import (
	"time"

	"github.com/ivanzzeth/go-universal-data-containers/metrics"
)

var (
	_ Storage         = (*StorageWithMetrics)(nil)
	_ StorageSnapshot = (*StorageSnapshotWithMetrics)(nil)
)

// It's a wrapper for Storage with Prometheus metrics
type StorageWithMetrics struct {
	storage  Storage
	snapshot StorageSnapshot
}

func NewStorageWithMetrics(storage Storage) StorageWithMetrics {
	return StorageWithMetrics{storage: storage, snapshot: NewStorageSnapshotWithMetrics(storage)}
}

func (s StorageWithMetrics) StorageType() string {
	return s.storage.StorageType()
}

func (s StorageWithMetrics) StorageName() string {
	return s.storage.StorageName()
}

func (s StorageWithMetrics) Lock() {
	s.storage.Lock()
}

func (s StorageWithMetrics) Unlock() {
	s.storage.Unlock()
}

func (s StorageWithMetrics) LoadState(name string, id string) (state State, err error) {
	metrics.MetricStateStorageOperationTotal.WithLabelValues(
		s.storage.StorageType(),
		string(metrics.StateStorageOperationLoadState),
	).Inc()

	startTime := time.Now()
	defer func() {
		metrics.MetricStateStorageOperationDuration.WithLabelValues(
			s.storage.StorageType(),
			string(metrics.StateStorageOperationLoadState),
		).Observe(time.Since(startTime).Seconds())

		if err != nil {
			metrics.MetricStateStorageOperationErrorTotal.WithLabelValues(
				s.storage.StorageType(),
				string(metrics.StateStorageOperationLoadState),
			).Inc()
		}
	}()

	return s.storage.LoadState(name, id)
}

func (s StorageWithMetrics) LoadAllStates() (state []State, err error) {
	metrics.MetricStateStorageOperationTotal.WithLabelValues(
		s.storage.StorageType(),
		string(metrics.StateStorageOperationLoadAllStates),
	).Inc()

	startTime := time.Now()
	defer func() {
		metrics.MetricStateStorageOperationDuration.WithLabelValues(
			s.storage.StorageType(),
			string(metrics.StateStorageOperationLoadAllStates),
		).Observe(time.Since(startTime).Seconds())

		if err != nil {
			metrics.MetricStateStorageOperationErrorTotal.WithLabelValues(
				s.storage.StorageType(),
				string(metrics.StateStorageOperationLoadAllStates),
			).Inc()
		}
	}()

	return s.storage.LoadAllStates()
}

func (s StorageWithMetrics) SaveStates(states ...State) (err error) {
	metrics.MetricStateStorageOperationTotal.WithLabelValues(
		s.storage.StorageType(),
		string(metrics.StateStorageOperationSaveStates),
	).Inc()

	startTime := time.Now()
	defer func() {
		metrics.MetricStateStorageOperationDuration.WithLabelValues(
			s.storage.StorageType(),
			string(metrics.StateStorageOperationSaveStates),
		).Observe(time.Since(startTime).Seconds())

		if err != nil {
			metrics.MetricStateStorageOperationErrorTotal.WithLabelValues(
				s.storage.StorageType(),
				string(metrics.StateStorageOperationSaveStates),
			).Inc()
		}
	}()

	return s.storage.SaveStates(states...)
}

func (s StorageWithMetrics) ClearAllStates() (err error) {
	metrics.MetricStateStorageOperationTotal.WithLabelValues(
		s.storage.StorageType(),
		string(metrics.StateStorageOperationClearAllStates),
	).Inc()

	startTime := time.Now()
	defer func() {
		metrics.MetricStateStorageOperationDuration.WithLabelValues(
			s.storage.StorageType(),
			string(metrics.StateStorageOperationClearAllStates),
		).Observe(time.Since(startTime).Seconds())

		if err != nil {
			metrics.MetricStateStorageOperationErrorTotal.WithLabelValues(
				s.storage.StorageType(),
				string(metrics.StateStorageOperationClearAllStates),
			).Inc()
		}
	}()

	return s.storage.ClearAllStates()
}

func (s StorageWithMetrics) GetStateIDs(name string) (ids []string, err error) {
	metrics.MetricStateStorageOperationTotal.WithLabelValues(
		s.storage.StorageType(),
		string(metrics.StateStorageOperationGetStateIDs),
	).Inc()

	startTime := time.Now()
	defer func() {
		metrics.MetricStateStorageOperationDuration.WithLabelValues(
			s.storage.StorageType(),
			string(metrics.StateStorageOperationGetStateIDs),
		).Observe(time.Since(startTime).Seconds())

		if err != nil {
			metrics.MetricStateStorageOperationErrorTotal.WithLabelValues(
				s.storage.StorageType(),
				string(metrics.StateStorageOperationGetStateIDs),
			).Inc()
		}
	}()

	return s.storage.GetStateIDs(name)
}

func (s StorageWithMetrics) GetStateNames() (names []string, err error) {
	metrics.MetricStateStorageOperationTotal.WithLabelValues(
		s.storage.StorageType(),
		string(metrics.StateStorageOperationGetStateNames),
	).Inc()

	startTime := time.Now()
	defer func() {
		metrics.MetricStateStorageOperationDuration.WithLabelValues(
			s.storage.StorageType(),
			string(metrics.StateStorageOperationGetStateNames),
		).Observe(time.Since(startTime).Seconds())

		if err != nil {
			metrics.MetricStateStorageOperationErrorTotal.WithLabelValues(
				s.storage.StorageType(),
				string(metrics.StateStorageOperationGetStateNames),
			).Inc()
		}
	}()

	return s.storage.GetStateNames()
}

func (s StorageWithMetrics) SetStorageForSnapshot(storage Storage) {
	s.snapshot.SetStorageForSnapshot(storage)
}

func (s StorageWithMetrics) GetStorageForSnapshot() (storage Storage) {
	return s.snapshot.GetStorageForSnapshot()
}

func (s StorageWithMetrics) SnapshotStates() (snapshotID string, err error) {
	return s.snapshot.SnapshotStates()
}

func (s StorageWithMetrics) RevertStatesToSnapshot(snapshotID string) (err error) {
	return s.snapshot.RevertStatesToSnapshot(snapshotID)
}

func (s StorageWithMetrics) GetSnapshot(snapshotID string) (storage Storage, err error) {
	return s.snapshot.GetSnapshot(snapshotID)
}

func (s StorageWithMetrics) DeleteSnapshot(snapshotID string) (err error) {
	return s.snapshot.DeleteSnapshot(snapshotID)
}

func (s StorageWithMetrics) ClearSnapshots() (err error) {
	return s.snapshot.ClearSnapshots()
}

type StorageSnapshotWithMetrics struct {
	snapshot StorageSnapshot
}

func NewStorageSnapshotWithMetrics(snapshot StorageSnapshot) *StorageSnapshotWithMetrics {
	return &StorageSnapshotWithMetrics{
		snapshot: snapshot,
	}
}

func (s StorageSnapshotWithMetrics) SetStorageForSnapshot(storage Storage) {
	s.snapshot.SetStorageForSnapshot(storage)
}

func (s StorageSnapshotWithMetrics) GetStorageForSnapshot() (storage Storage) {
	return s.snapshot.GetStorageForSnapshot()
}

func (s StorageSnapshotWithMetrics) SnapshotStates() (snapshotID string, err error) {
	metrics.MetricStateSnapshotOperationTotal.WithLabelValues(
		s.snapshot.GetStorageForSnapshot().StorageType(),
		string(metrics.StateSnapshotOperationSnapshotStates),
	).Inc()

	startTime := time.Now()
	defer func() {
		metrics.MetricStateSnapshotOperationDuration.WithLabelValues(
			s.snapshot.GetStorageForSnapshot().StorageType(),
			string(metrics.StateSnapshotOperationSnapshotStates),
		).Observe(time.Since(startTime).Seconds())

		if err != nil {
			metrics.MetricStateSnapshotOperationErrorTotal.WithLabelValues(
				s.snapshot.GetStorageForSnapshot().StorageType(),
				string(metrics.StateSnapshotOperationSnapshotStates),
			).Inc()
		}
	}()

	return s.snapshot.SnapshotStates()
}

func (s StorageSnapshotWithMetrics) RevertStatesToSnapshot(snapshotID string) (err error) {
	metrics.MetricStateSnapshotOperationTotal.WithLabelValues(
		s.snapshot.GetStorageForSnapshot().StorageType(),
		string(metrics.StateSnapshotOperationRevertStatesToSnapshot),
	).Inc()

	startTime := time.Now()
	defer func() {
		metrics.MetricStateSnapshotOperationDuration.WithLabelValues(
			s.snapshot.GetStorageForSnapshot().StorageType(),
			string(metrics.StateSnapshotOperationRevertStatesToSnapshot),
		).Observe(time.Since(startTime).Seconds())

		if err != nil {
			metrics.MetricStateSnapshotOperationErrorTotal.WithLabelValues(
				s.snapshot.GetStorageForSnapshot().StorageType(),
				string(metrics.StateSnapshotOperationRevertStatesToSnapshot),
			).Inc()
		}
	}()

	return s.snapshot.RevertStatesToSnapshot(snapshotID)
}

func (s StorageSnapshotWithMetrics) GetSnapshot(snapshotID string) (storage Storage, err error) {
	metrics.MetricStateSnapshotOperationTotal.WithLabelValues(
		s.snapshot.GetStorageForSnapshot().StorageType(),
		string(metrics.StateSnapshotOperationGetSnapshot),
	).Inc()

	startTime := time.Now()
	defer func() {
		metrics.MetricStateSnapshotOperationDuration.WithLabelValues(
			s.snapshot.GetStorageForSnapshot().StorageType(),
			string(metrics.StateSnapshotOperationGetSnapshot),
		).Observe(time.Since(startTime).Seconds())

		if err != nil {
			metrics.MetricStateSnapshotOperationErrorTotal.WithLabelValues(
				s.snapshot.GetStorageForSnapshot().StorageType(),
				string(metrics.StateSnapshotOperationGetSnapshot),
			).Inc()
		}
	}()

	return s.snapshot.GetSnapshot(snapshotID)
}

func (s StorageSnapshotWithMetrics) DeleteSnapshot(snapshotID string) (err error) {
	metrics.MetricStateSnapshotOperationTotal.WithLabelValues(
		s.snapshot.GetStorageForSnapshot().StorageType(),
		string(metrics.StateSnapshotOperationDeleteSnapshot),
	).Inc()

	startTime := time.Now()
	defer func() {
		metrics.MetricStateSnapshotOperationDuration.WithLabelValues(
			s.snapshot.GetStorageForSnapshot().StorageType(),
			string(metrics.StateSnapshotOperationDeleteSnapshot),
		).Observe(time.Since(startTime).Seconds())

		if err != nil {
			metrics.MetricStateSnapshotOperationErrorTotal.WithLabelValues(
				s.snapshot.GetStorageForSnapshot().StorageType(),
				string(metrics.StateSnapshotOperationDeleteSnapshot),
			).Inc()
		}
	}()

	return s.snapshot.DeleteSnapshot(snapshotID)
}

func (s StorageSnapshotWithMetrics) ClearSnapshots() (err error) {
	metrics.MetricStateSnapshotOperationTotal.WithLabelValues(
		s.snapshot.GetStorageForSnapshot().StorageType(),
		string(metrics.StateSnapshotOperationClearSnapshots),
	).Inc()

	startTime := time.Now()
	defer func() {
		metrics.MetricStateSnapshotOperationDuration.WithLabelValues(
			s.snapshot.GetStorageForSnapshot().StorageType(),
			string(metrics.StateSnapshotOperationClearSnapshots),
		).Observe(time.Since(startTime).Seconds())

		if err != nil {
			metrics.MetricStateSnapshotOperationErrorTotal.WithLabelValues(
				s.snapshot.GetStorageForSnapshot().StorageType(),
				string(metrics.StateSnapshotOperationClearSnapshots),
			).Inc()
		}
	}()

	return s.snapshot.ClearSnapshots()
}
