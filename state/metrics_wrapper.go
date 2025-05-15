package state

import (
	"context"
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

func (s StorageWithMetrics) Lock(ctx context.Context) error {
	return s.storage.Lock(ctx)
}

func (s StorageWithMetrics) Unlock(ctx context.Context) error {
	return s.storage.Unlock(ctx)
}

func (s StorageWithMetrics) LoadState(ctx context.Context, name string, id string) (state State, err error) {
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

	return s.storage.LoadState(ctx, name, id)
}

func (s StorageWithMetrics) LoadAllStates(ctx context.Context) (state []State, err error) {
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

	return s.storage.LoadAllStates(ctx)
}

func (s StorageWithMetrics) SaveStates(ctx context.Context, states ...State) (err error) {
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

	return s.storage.SaveStates(ctx, states...)
}

func (s StorageWithMetrics) ClearStates(ctx context.Context, states ...State) (err error) {
	metrics.MetricStateStorageOperationTotal.WithLabelValues(
		s.storage.StorageType(),
		string(metrics.StateStorageOperationClearStates),
	).Inc()

	startTime := time.Now()
	defer func() {
		metrics.MetricStateStorageOperationDuration.WithLabelValues(
			s.storage.StorageType(),
			string(metrics.StateStorageOperationClearStates),
		).Observe(time.Since(startTime).Seconds())

		if err != nil {
			metrics.MetricStateStorageOperationErrorTotal.WithLabelValues(
				s.storage.StorageType(),
				string(metrics.StateStorageOperationClearStates),
			).Inc()
		}
	}()

	return s.storage.ClearStates(ctx, states...)
}

func (s StorageWithMetrics) ClearAllStates(ctx context.Context) (err error) {
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

	return s.storage.ClearAllStates(ctx)
}

func (s StorageWithMetrics) GetStateIDs(ctx context.Context, name string) (ids []string, err error) {
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

	return s.storage.GetStateIDs(ctx, name)
}

func (s StorageWithMetrics) GetStateNames(ctx context.Context) (names []string, err error) {
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

	return s.storage.GetStateNames(ctx)
}

func (s StorageWithMetrics) SetStorageForSnapshot(storage Storage) {
	s.snapshot.SetStorageForSnapshot(storage)
}

func (s StorageWithMetrics) GetStorageForSnapshot() (storage Storage) {
	return s.snapshot.GetStorageForSnapshot()
}

func (s StorageWithMetrics) SnapshotStates(ctx context.Context) (snapshotID string, err error) {
	return s.snapshot.SnapshotStates(ctx)
}

func (s StorageWithMetrics) RevertStatesToSnapshot(ctx context.Context, snapshotID string) (err error) {
	return s.snapshot.RevertStatesToSnapshot(ctx, snapshotID)
}

func (s StorageWithMetrics) GetSnapshot(ctx context.Context, snapshotID string) (storage Storage, err error) {
	return s.snapshot.GetSnapshot(ctx, snapshotID)
}

func (s StorageWithMetrics) GetSnapshotIDs(ctx context.Context) (snapshotIDs []string, err error) {
	return s.snapshot.GetSnapshotIDs(ctx)
}

func (s StorageWithMetrics) DeleteSnapshot(ctx context.Context, snapshotID string) (err error) {
	return s.snapshot.DeleteSnapshot(ctx, snapshotID)
}

func (s StorageWithMetrics) ClearSnapshots(ctx context.Context) (err error) {
	return s.snapshot.ClearSnapshots(ctx)
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

func (s StorageSnapshotWithMetrics) SnapshotStates(ctx context.Context) (snapshotID string, err error) {
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

	return s.snapshot.SnapshotStates(ctx)
}

func (s StorageSnapshotWithMetrics) RevertStatesToSnapshot(ctx context.Context, snapshotID string) (err error) {
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

	return s.snapshot.RevertStatesToSnapshot(ctx, snapshotID)
}

func (s StorageSnapshotWithMetrics) GetSnapshot(ctx context.Context, snapshotID string) (storage Storage, err error) {
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

	return s.snapshot.GetSnapshot(ctx, snapshotID)
}

func (s StorageSnapshotWithMetrics) GetSnapshotIDs(ctx context.Context) (snapshotIDs []string, err error) {
	metrics.MetricStateSnapshotOperationTotal.WithLabelValues(
		s.snapshot.GetStorageForSnapshot().StorageType(),
		string(metrics.StateSnapshotOperationGetSnapshotIDs),
	).Inc()

	startTime := time.Now()
	defer func() {
		metrics.MetricStateSnapshotOperationDuration.WithLabelValues(
			s.snapshot.GetStorageForSnapshot().StorageType(),
			string(metrics.StateSnapshotOperationGetSnapshotIDs),
		).Observe(time.Since(startTime).Seconds())

		if err != nil {
			metrics.MetricStateSnapshotOperationErrorTotal.WithLabelValues(
				s.snapshot.GetStorageForSnapshot().StorageType(),
				string(metrics.StateSnapshotOperationGetSnapshotIDs),
			).Inc()
		}
	}()

	return s.snapshot.GetSnapshotIDs(ctx)
}

func (s StorageSnapshotWithMetrics) DeleteSnapshot(ctx context.Context, snapshotID string) (err error) {
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

	return s.snapshot.DeleteSnapshot(ctx, snapshotID)
}

func (s StorageSnapshotWithMetrics) ClearSnapshots(ctx context.Context) (err error) {
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

	return s.snapshot.ClearSnapshots(ctx)
}
