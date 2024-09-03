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
	stdoutFailedMessages    = "failed_messages"
	stdoutTargetQueueLength = "target_queue_length"
	stdoutTotalMessages     = "total_messages"
	stdoutGlobalQueueSize   = "global_queue_size"
	stdoutTotalTargets      = "total_targets"
)

var (
	stdoutFailedMessagesMD = NewCounterMD(stdoutFailedMessages,
		"Total number of logs that failed to send since start",
		targetID)
	stdoutTargetQueueLengthMD = NewGaugeMD(stdoutTargetQueueLength,
		"Number of unsent logs in queue for the specific target target",
		targetID)
	stdoutTotalMessagesMD = NewCounterMD(stdoutTotalMessages,
		"Total number of logs sent since start",
		targetID)
	stdoutGlobalQueueSizeMD = NewCounterMD(stdoutGlobalQueueSize,
		"Total number of logs in queue for all targets",
		targetID)
	stdoutTotalTargetsMD = NewCounterMD(stdoutTotalTargets,
		"Total number of system endpoint targets",
		targetID)
)

// loadSTDOutMetrics - `MetricsLoaderFn` for stdout
func loadSTDOutMetrics(_ context.Context, m MetricValues, c *metricsCache) error {
	s := logger.STDOutStatsV3()
	labels := []string{targetID, totalID}
	m.Set(stdoutGlobalQueueSize, float64(s.GlobalQueueSize), labels...)
	m.Set(stdoutTotalTargets, float64(s.TotalTargets), labels...)

	// m.Set(stdoutTargetQueueLength, float64(s.Totals.QueueLength), labels...)
	//
	// m.Set(stdoutTotalMessages, float64(s.Totals.TotalMessages), labels...)
	// m.Set(stdoutFailedMessages, float64(s.Totals.FailedMessages), labels...)

	for id, st := range s.Targets {
		labels := []string{targetID, id}
		m.Set(stdoutTargetQueueLength, float64(st.QueueLength), labels...)
		m.Set(stdoutFailedMessages, float64(st.FailedMessages), labels...)
		m.Set(stdoutTotalMessages, float64(st.TotalMessages), labels...)
	}

	return nil
}
