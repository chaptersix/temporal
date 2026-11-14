package chasm

// workCounter identifies test-only observations at the actual operation sites.
// Production builds inline the empty observation methods.
type workCounter uint8

const (
	workColdNodes workCounter = iota
	workTreeVisits
	workSyncVisits
	workMapHydrationKeys
	workMapSyncKeys
	workMapKeyCopies
	workDeleteChildVisits
	workDecodeCalls
	workDecodeBytes
	workDataDecodeCalls
	workSerializeCalls
	workSerializeBytes
	workTaskValidations
	workSerializedNodeProtoBytes
	workSetEnqueues
	workSetDrains
	workSlotVisits
	workPathInspections
	workCounterCount
)
