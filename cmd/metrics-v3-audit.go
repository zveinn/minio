// Copyright (c) 2015-2024 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"

	"github.com/minio/minio/internal/logger"
)

const (
	auditFailedMessages           = "failed_messages"
	auditTargetQueueLength        = "target_queue_length"
	auditTotalMessages            = "total_messages"
	targetID                      = "target_id"
	totalID                       = "total"
	auditGlobalQueueSize          = "global_queue_size"
	auditTotalTargets             = "total_targets"
	auditTotalBatchesSentToStdout = "total_batches_to_stdout"
	auditRequestsTotal            = "total_requests"
	auditRequestsFailed           = "failed_requests"
	auditWorkerCount              = "worker_count"
)

var (
	auditFailedMessagesMD = NewCounterMD(auditFailedMessages,
		"Total number of messages that failed to send since start",
		targetID)
	auditTargetQueueLengthMD = NewGaugeMD(auditTargetQueueLength,
		"Number of unsent messages or batches in queue for target",
		targetID)
	auditTotalMessagesMD = NewCounterMD(auditTotalMessages,
		"Total number of messages sent since start",
		targetID)
	auditGlobalQueueSizeMD = NewCounterMD(auditGlobalQueueSize,
		"Total number of messages in queue for all targets",
		targetID)
	auditTotalTargetsMD = NewCounterMD(auditTotalTargets,
		"Total number of audit endpoint targets",
		targetID)

	auditRequestsTotalMD = NewCounterMD(auditRequestsTotal,
		"Total number of requests made to the audit endpoint",
		targetID)

	auditRequestsFailedMD = NewCounterMD(auditRequestsFailed,
		"Total number of requests failed sending to the audit endpoint",
		targetID)

	auditWorkerCountMD = NewCounterMD(auditWorkerCount,
		"Total number of async workers",
		targetID)
)

// loadAuditMetrics - `MetricsLoaderFn` for audit
func loadAuditMetrics(_ context.Context, m MetricValues, c *metricsCache) error {
	s := logger.AduitStatsV3()
	labels := []string{targetID, totalID}
	m.Set(auditGlobalQueueSize, float64(s.GlobalQueueSize), labels...)
	m.Set(auditTotalTargets, float64(s.TotalTargets), labels...)

	// m.Set(auditTargetQueueLength, float64(s.Totals.QueueLength), labels...)
	//
	// m.Set(auditTotalMessages, float64(s.Totals.TotalMessages), labels...)
	// m.Set(auditFailedMessages, float64(s.Totals.FailedMessages), labels...)
	//
	// m.Set(auditRequestsTotal, float64(s.Totals.TotalRequests), labels...)
	// m.Set(auditRequestsFailed, float64(s.Totals.FailedRequests), labels...)
	//
	// m.Set(auditWorkerCount, float64(s.Totals.TotalWorkers), labels...)

	for id, st := range s.Targets {
		labels := []string{targetID, id}
		m.Set(auditFailedMessages, float64(st.FailedMessages), labels...)
		m.Set(auditTargetQueueLength, float64(st.QueueLength), labels...)
		m.Set(auditTotalMessages, float64(st.TotalMessages), labels...)
		m.Set(auditRequestsTotal, float64(st.TotalRequests), labels...)
		m.Set(auditRequestsFailed, float64(st.FailedRequests), labels...)
		m.Set(auditWorkerCount, float64(st.TotalWorkers), labels...)
	}

	return nil
}
