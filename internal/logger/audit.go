// Copyright (c) 2015-2021 MinIO, Inc.
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
	"net/http"
	"strconv"
	"time"

	internalAudit "github.com/minio/minio/internal/logger/message/audit"
	"github.com/minio/minio/internal/mcontext"
	"github.com/minio/pkg/v3/logger/message/audit"

	xhttp "github.com/minio/minio/internal/http"
)

const contextAuditKey = contextKeyType("audit-entry")

// SetAuditEntry sets Audit info in the context.
func SetAuditEntry(ctx context.Context, audit *audit.Entry) context.Context {
	if ctx == nil {
		LogIf(context.Background(), "audit", fmt.Errorf("context is nil"))
		return nil
	}
	return context.WithValue(ctx, contextAuditKey, audit)
}

// GetAuditEntry returns Audit entry if set.
func GetAuditEntry(ctx context.Context) *audit.Entry {
	if ctx != nil {
		r, ok := ctx.Value(contextAuditKey).(*audit.Entry)
		if ok {
			return r
		}
		r = &audit.Entry{
			Version:      internalAudit.Version,
			DeploymentID: xhttp.GlobalDeploymentID,
			Time:         time.Now().UTC(),
		}
		return r
	}
	return nil
}

// AuditLog - logs audit logs to all audit targets.
func AuditLog(ctx context.Context, w http.ResponseWriter, r *http.Request, reqClaims map[string]interface{}, filterKeys ...string) {
	auditEntry := new(audit.Entry)
	if w == nil || r == nil {
		auditEntry := GetAuditEntry(ctx)
		if auditEntry == nil {
			return
		}
		select {
		case GlobalAuditLogger.Ch <- auditEntry:
		default:
			LogOnceIf(ctx, "logging", fmt.Errorf("Unable place audit entry on global channel, placed in system log channel instead"), "send-audit-event-failure")
		}
		return
	}

	reqInfo := GetReqInfo(ctx)
	if reqInfo == nil {
		return
	}
	reqInfo.RLock()
	defer reqInfo.RUnlock()

	auditEntry = internalAudit.ToEntry(w, r, reqClaims, xhttp.GlobalDeploymentID)
	auditEntry.Trigger = "incoming"

	for _, filterKey := range filterKeys {
		delete(auditEntry.ReqClaims, filterKey)
		delete(auditEntry.ReqQuery, filterKey)
		delete(auditEntry.ReqHeader, filterKey)
		delete(auditEntry.RespHeader, filterKey)
	}

	var (
		statusCode      int
		timeToResponse  time.Duration
		timeToFirstByte time.Duration
	)

	auditEntry.API.OutputBytes = -1 // -1: unknown output bytes

	tc, ok := r.Context().Value(mcontext.ContextTraceKey).(*mcontext.TraceCtxt)
	if ok {
		statusCode = tc.ResponseRecorder.StatusCode
		auditEntry.API.OutputBytes = int64(tc.ResponseRecorder.Size())
		auditEntry.API.HeaderBytes = int64(tc.ResponseRecorder.HeaderSize())
		timeToResponse = time.Now().UTC().Sub(tc.ResponseRecorder.StartTime)
		timeToFirstByte = tc.ResponseRecorder.TimeToFirstByte
	}

	auditEntry.AccessKey = reqInfo.Cred.AccessKey
	auditEntry.ParentUser = reqInfo.Cred.ParentUser

	auditEntry.API.Name = reqInfo.API
	auditEntry.API.Bucket = reqInfo.BucketName
	auditEntry.API.Object = reqInfo.ObjectName
	auditEntry.API.Objects = make([]audit.ObjectVersion, 0, len(reqInfo.Objects))
	for _, ov := range reqInfo.Objects {
		auditEntry.API.Objects = append(auditEntry.API.Objects, audit.ObjectVersion{
			ObjectName: ov.ObjectName,
			VersionID:  ov.VersionID,
		})
	}
	auditEntry.API.Status = http.StatusText(statusCode)
	auditEntry.API.StatusCode = statusCode
	auditEntry.API.InputBytes = r.ContentLength
	auditEntry.API.TimeToResponse = strconv.FormatInt(timeToResponse.Nanoseconds(), 10) + "ns"
	auditEntry.API.TimeToResponseInNS = strconv.FormatInt(timeToResponse.Nanoseconds(), 10)
	// We hold the lock, so we cannot call reqInfo.GetTagsMap().
	tags := make(map[string]interface{}, len(reqInfo.tags))
	for _, t := range reqInfo.tags {
		tags[t.Key] = t.Val
	}
	auditEntry.Tags = tags
	// ignore cases for ttfb when its zero.
	if timeToFirstByte != 0 {
		auditEntry.API.TimeToFirstByte = strconv.FormatInt(timeToFirstByte.Nanoseconds(), 10) + "ns"
		auditEntry.API.TimeToFirstByteInNS = strconv.FormatInt(timeToFirstByte.Nanoseconds(), 10)
	}

	select {
	case GlobalAuditLogger.Ch <- auditEntry:
	default:
		LogOnceIf(ctx, "logging", fmt.Errorf("Unable place audit entry on global channel, placed in system log channel instead"), "send-audit-event-failure")
	}
}
