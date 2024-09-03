package logger

import (
	"sync/atomic"

	"github.com/minio/minio/internal/logger/target/types"
)

type QueueTargetStats struct {
	name       string
	endpoint   string
	state      int64
	targetType types.TargetType
	base       types.TargetStats
}

func (e *QueueTargetStats) IsOnline() bool {
	if e.state > 0 {
		return true
	} else {
		return false
	}
}

func (e *QueueTargetStats) Name() string {
	return e.name
}

func (e *QueueTargetStats) Endpoint() string {
	return e.endpoint
}

func (e *QueueTargetStats) Type() types.TargetType {
	return e.targetType
}

func (e *QueueTargetStats) Stats() types.TargetStats {
	return e.base
}

func (g *GlobalQueue[_]) GetTargetStats(nameFilter string) (stats []QueueTargetStats) {
	stats = make([]QueueTargetStats, 0)

	for _, v := range g.Targets {
		if nameFilter != "" && v.name != nameFilter {
			continue
		}
		r := v.runtime.Load()
		s := QueueTargetStats{
			name:       v.name,
			targetType: r.Type,
			endpoint:   r.Endpoint,
			base: types.TargetStats{
				TotalMessages:  atomic.LoadInt64(&v.TotalMessages),
				FailedMessages: atomic.LoadInt64(&v.FailedMessages),
				FailedRequests: atomic.LoadInt64(&v.FailedRequests),
				TotalRequests:  atomic.LoadInt64(&v.TotalRequests),
				TotalWorkers:   v.workerCount,
				QueueLength:    len(v.Ch),
			},
		}
		stats = append(stats, s)
	}

	return
}
