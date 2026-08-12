package cache

import (
	"go.temporal.io/server/common/definition"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
)

// GetMutableState returns the MutableState for the given key from the cache.
// Exported for testing purposes.
func GetMutableState(cache Cache, key Key) historyi.MutableState {
	return getWorkflowContext(cache, key).(*workflow.ContextImpl).MutableState
}

// PutContextIfNotExist puts the given workflow Context into the cache, if it doens't already exist.
// Exported for testing purposes.
func PutContextIfNotExist(cache Cache, key Key, value historyi.WorkflowContext) error {
	_, err := cache.(*cacheImpl).PutIfNotExist(key, &cacheItem{wfContext: value})
	return err
}

// ClearMutableState clears cached mutable state for the given key to
// force a reload from persistence on the next access.
func ClearMutableState(cache Cache, key Key) {
	getWorkflowContext(cache, key).Clear()
}

// EvictWorkflowExecution removes the cached workflow context through the cache eviction callback.
// Delete, capacity eviction, and TTL eviction use the same internal removal path. This helper
// bypasses only the cache's victim-selection policy, so callers are responsible for invoking it
// after the entry's workflow lease has been released, when production eviction could select it.
func EvictWorkflowExecution(cache Cache, workflowKey definition.WorkflowKey) bool {
	cacheImpl := cache.(*cacheImpl)
	iterator := cacheImpl.Iterator()
	var keyToDelete *Key
	for iterator.HasNext() {
		key := iterator.Next().Key().(Key)
		if key.WorkflowKey == workflowKey {
			keyToDelete = &key
			break
		}
	}
	iterator.Close()

	if keyToDelete == nil {
		return false
	}
	cacheImpl.Delete(*keyToDelete)
	return true
}

func getWorkflowContext(cache Cache, key Key) historyi.WorkflowContext {
	return cache.(*cacheImpl).Get(key).(*cacheItem).wfContext
}
