package sync

import (
	"time"

	wfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
)

type EmptyThrottler struct {
}

func (e EmptyThrottler) Init(wfs []wfv1.Workflow) error {
	return nil
}

func (e EmptyThrottler) Add(key Key, priority int32, creationTime time.Time) {
	return
}

func (e EmptyThrottler) Admit(key Key) bool {
	return true
}

func (e EmptyThrottler) Remove(key Key) {
}

func (e EmptyThrottler) UpdateParallelism(limit int) {
}

func (e EmptyThrottler) UpdateNamespaceParallelism(namespace string, limit int) {
}

func (e EmptyThrottler) ResetNamespaceParallelism(namespace string) {
}
