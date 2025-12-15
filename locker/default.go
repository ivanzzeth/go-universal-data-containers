package locker

var (
	defaultSyncLockerGenerator SyncLockerGenerator   = NewMemoryLockerGenerator()
	defaultRWLockerGenerator   SyncRWLockerGenerator = NewMemoryRWLockerGenerator()
)

func SetDefaultSyncLockerGenerator(lockerGenerator SyncLockerGenerator) {
	defaultSyncLockerGenerator = lockerGenerator
}

func GetDefaultSyncLockerGenerator() SyncLockerGenerator {
	return defaultSyncLockerGenerator
}

func SetDefaultRWLockerGenerator(lockerGenerator SyncRWLockerGenerator) {
	defaultRWLockerGenerator = lockerGenerator
}

func GetDefaultRWLockerGenerator() SyncRWLockerGenerator {
	return defaultRWLockerGenerator
}
