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
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/minio/minio/internal/logger/target/types"
)

type Trgt struct {
	batchSize int
	// typePrefix string
	Writer          io.Writer
	InterfaceWriter InterfaceWriter
	BatchWriter     BatchInterfaceWriter
	Type            types.TargetType
	endpoint        string

	cancelFunc context.CancelFunc
	ctx        context.Context

	// status         int32
	LogOnceIf func(ctx context.Context, err error, id string, errKind ...interface{}) `json:"-"`

	// to be deprecated
	// queueSize int
	// we still use queueDir during transitions
	// from the old logging system to the new.
	queueDir string
}

func (t *Trgt) Cancel() {
	t.cancelFunc()
}

func (t *Trgt) Name() string {
	return fmt.Sprintf("minio-%s-%s", t.Type, t.Name)
}

// func (h *Trgt) Send(entry interface{}) error {
// 	return nil
// }

// This is always set represent /dev/console target
// var consoleTgt Target

// CurrentStats returns the current statistics.
func CurrentStats() map[string]types.TargetStats {
	audit := GlobalAuditLogger.GetTargetStats("")
	sys := GlobalSystemLogger.GetTargetStats("")

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
