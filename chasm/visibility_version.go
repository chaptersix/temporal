package chasm

import (
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/proto"
)

// refreshMemoCache observes persisted state on cold construction or replication.
// Existing persisted visibility tasks remain the authority for pending export.
func (n *Node) refreshMemoCache(ctx Context, component Component) error {
	if provider, ok := component.(VisibilityMemoVersionProvider); ok {
		version := provider.MemoVersion(ctx)
		if version == 0 {
			return serviceerror.NewInternal("visibility memo version must be nonzero")
		}
		n.currentMemoVersion = version
		n.hasCurrentMemoVersion = true
		n.currentMemo = nil
		return nil
	}
	n.hasCurrentMemoVersion = false
	n.currentMemoVersion = 0
	if provider, ok := component.(VisibilityMemoProvider); ok {
		n.currentMemo = proto.Clone(provider.Memo(ctx))
	}
	return nil
}

func (n *Node) updateMemoCache(ctx Context, component Component) (bool, error) {
	if provider, ok := component.(VisibilityMemoVersionProvider); ok {
		version := provider.MemoVersion(ctx)
		if version == 0 {
			return false, serviceerror.NewInternal("visibility memo version must be nonzero")
		}
		if n.hasCurrentMemoVersion && version < n.currentMemoVersion {
			return false, serviceerror.NewInternal("visibility memo version must not decrease or wrap")
		}
		changed := !n.hasCurrentMemoVersion || version != n.currentMemoVersion
		n.currentMemoVersion = version
		n.hasCurrentMemoVersion = true
		n.currentMemo = nil
		return changed, nil
	}
	if provider, ok := component.(VisibilityMemoProvider); ok {
		memo := provider.Memo(ctx)
		changed := !proto.Equal(n.currentMemo, memo)
		n.currentMemo = proto.Clone(memo)
		return changed, nil
	}
	return false, nil
}
