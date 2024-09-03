// Copyright (c) 2015-2022 MinIO, Inc.
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

package logger

import (
	"fmt"
	"strings"

	"github.com/minio/minio/internal/logger/target/types"
)

// This is always set represent /dev/console target
// var consoleTgt Target
type queueTargetStatsV3 struct {
	GlobalQueueSize int
	TotalTargets    int
	Targets         map[string]types.TargetStats
	// Totals          types.TargetStats
}

func STDOutStatsV3() (s queueTargetStatsV3) {
	targets := GlobalSTDOutLogger.GetTargetStats("")
	s.GlobalQueueSize = GlobalSTDOutLogger.QueueSize()
	s.TotalTargets = int(GlobalSTDOutLogger.TargetCount.Load())
	s.Targets = make(map[string]types.TargetStats)

	for _, t := range targets {
		key := strings.ToLower(fmt.Sprintf("%s_%s", t.Type().String(), t.Name()))
		s.Targets[key] = t.Stats()
		// s.Totals.QueueLength += s.Targets[key].QueueLength
		//
		// s.Totals.TotalMessages += s.Targets[key].TotalMessages
		// s.Totals.FailedMessages += s.Targets[key].FailedMessages
	}

	return
}

func AduitStatsV3() (s queueTargetStatsV3) {
	targets := GlobalAuditLogger.GetTargetStats("")
	s.GlobalQueueSize = GlobalAuditLogger.QueueSize()
	s.TotalTargets = int(GlobalAuditLogger.TargetCount.Load())
	s.Targets = make(map[string]types.TargetStats)

	for _, t := range targets {
		key := strings.ToLower(fmt.Sprintf("%s_%s", t.Type().String(), t.Name()))
		s.Targets[key] = t.Stats()
		// s.Totals.QueueLength += s.Targets[key].QueueLength
		//
		// s.Totals.TotalMessages += s.Targets[key].TotalMessages
		// s.Totals.FailedMessages += s.Targets[key].FailedMessages
		//
		// s.Totals.TotalRequests += s.Targets[key].TotalRequests
		// s.Totals.FailedRequests += s.Targets[key].FailedRequests
		//
		// s.Totals.TotalWorkers += s.Targets[key].TotalWorkers
	}

	return
}

// CurrentStats returns the current statistics.
func CurrentStats() map[string]types.TargetStats {
	audit := GlobalAuditLogger.GetTargetStats("")
	sys := GlobalSTDOutLogger.GetTargetStats("")

	// audit
	res := make(map[string]types.TargetStats, len(sys)+len(audit))
	cnt := make(map[string]int, len(sys)+len(audit))

	// Add system and audit.
	for _, t := range sys {
		key := strings.ToLower(t.Type().String())
		n := cnt[key]
		cnt[key]++
		key = fmt.Sprintf("sys_%s_%d", key, n)
		res[key] = t.Stats()
	}

	for _, t := range audit {
		key := strings.ToLower(t.Type().String())
		n := cnt[key]
		cnt[key]++
		key = fmt.Sprintf("audit_%s_%d", key, n)
		res[key] = t.Stats()
	}

	return res
}
