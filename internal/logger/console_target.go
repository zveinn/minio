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
	"container/ring"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/color"
	"github.com/minio/minio/internal/pubsub"
	"github.com/minio/pkg/v2/logger/message/log"
	xnet "github.com/minio/pkg/v2/net"
)

// number of log messages to buffer
const defaultLogBufferCount = 10000

// HTTPConsoleTarget holds global console logger state
type HTTPConsoleTarget struct {
	nodeName             string
	isDistributedErasure bool
	globalLocalNodeName  string

	sync.RWMutex
	pubsub *pubsub.PubSub[log.Info, madmin.LogMask]
	logBuf *ring.Ring
	Writer io.Writer
}

// NewConsoleLogger - creates new HTTPConsoleLoggerSys with all nodes subscribed to
// the console logging pub sub system
func NewConsoleLogger(
	ctx context.Context,
	isDistributedErasure bool,
	globalLocalNodeName string,
	writer io.Writer,
) *HTTPConsoleTarget {
	return &HTTPConsoleTarget{
		pubsub: pubsub.New[log.Info, madmin.LogMask](8),
		logBuf: ring.New(defaultLogBufferCount),
		Writer: writer,
	}
}

// SetNodeName - sets the node name if any after distributed setup has initialized
func (sys *HTTPConsoleTarget) SetNodeName(nodeName string) {
	if !sys.isDistributedErasure {
		sys.nodeName = ""
		return
	}

	host, err := xnet.ParseHost(sys.globalLocalNodeName)
	if err != nil {
		FatalIf(err, "Unable to start console logging subsystem")
	}

	sys.nodeName = host.Name
}

// Subscribe starts console logging for this node.
func (sys *HTTPConsoleTarget) Subscribe(subCh chan log.Info, doneCh <-chan struct{}, node string, last int, logKind madmin.LogMask, filter func(entry log.Info) bool) error {
	// Enable console logging for remote client.

	cnt := 0
	// by default send all console logs in the ring buffer unless node or limit query parameters
	// are set.
	var lastN []log.Info
	if last > defaultLogBufferCount || last <= 0 {
		last = defaultLogBufferCount
	}

	lastN = make([]log.Info, last)
	sys.RLock()
	sys.logBuf.Do(func(p interface{}) {
		if p != nil {
			lg, ok := p.(log.Info)
			if ok && lg.SendLog(node, logKind) {
				lastN[cnt%last] = lg
				cnt++
			}
		}
	})
	sys.RUnlock()
	// send last n console log messages in order filtered by node
	if cnt > 0 {
		for i := 0; i < last; i++ {
			entry := lastN[(cnt+i)%last]
			if (entry == log.Info{}) {
				continue
			}
			select {
			case subCh <- entry:
			case <-doneCh:
				return nil
			}
		}
	}
	return sys.pubsub.Subscribe(madmin.LogMaskAll, subCh, doneCh, filter)
}

// Send log message 'e' to console and publish to console
// log pubsub system
func (sys *HTTPConsoleTarget) Write(entry interface{}) error {
	var lg log.Info
	switch e := entry.(type) {
	case log.Entry:
		lg = log.Info{Entry: e, NodeName: sys.nodeName}
	case string:
		lg = log.Info{ConsoleMsg: e, NodeName: sys.nodeName}
	}
	sys.pubsub.Publish(lg)
	sys.Lock()
	sys.logBuf.Value = lg
	sys.logBuf = sys.logBuf.Next()
	sys.Unlock()
	return sys.Send(entry)
}

// Send log message 'e' to console
func (c *HTTPConsoleTarget) Send(e interface{}) error {
	entry, ok := e.(log.Entry)
	if !ok {
		return fmt.Errorf("Uexpected log entry structure %#v", e)
	}

	if IsJSON() {
		logJSON, err := json.Marshal(&entry)
		if err != nil {
			return err
		}
		// fmt.Println(string(logJSON))
		fmt.Fprintln(c.Writer, string(logJSON))
		return nil
	}

	if entry.Level == EventKind {
		fmt.Fprintln(c.Writer, entry.Message)
		return nil
	}

	traceLength := len(entry.Trace.Source)
	trace := make([]string, traceLength)

	// Add a sequence number and formatting for each stack trace
	// No formatting is required for the first entry
	for i, element := range entry.Trace.Source {
		trace[i] = fmt.Sprintf("%8v: %s", traceLength-i, element)
	}

	tagString := ""
	for key, value := range entry.Trace.Variables {
		if value != "" {
			if tagString != "" {
				tagString += ", "
			}
			tagString += fmt.Sprintf("%s=%#v", key, value)
		}
	}

	var apiString string
	if entry.API != nil {
		apiString = "API: " + entry.API.Name
		if entry.API.Args != nil {
			args := ""
			if entry.API.Args.Bucket != "" {
				args = args + "bucket=" + entry.API.Args.Bucket
			}
			if entry.API.Args.Object != "" {
				args = args + ", object=" + entry.API.Args.Object
			}
			if entry.API.Args.VersionID != "" {
				args = args + ", versionId=" + entry.API.Args.VersionID
			}
			if len(entry.API.Args.Objects) > 0 {
				args = args + ", multiObject=true, numberOfObjects=" + strconv.Itoa(len(entry.API.Args.Objects))
			}
			if len(args) > 0 {
				apiString += "(" + args + ")"
			}
		}
	} else {
		apiString = "INTERNAL"
	}
	timeString := "Time: " + entry.Time.Format(TimeFormat)

	var deploymentID string
	if entry.DeploymentID != "" {
		deploymentID = "\nDeploymentID: " + entry.DeploymentID
	}

	var requestID string
	if entry.RequestID != "" {
		requestID = "\nRequestID: " + entry.RequestID
	}

	var remoteHost string
	if entry.RemoteHost != "" {
		remoteHost = "\nRemoteHost: " + entry.RemoteHost
	}

	var host string
	if entry.Host != "" {
		host = "\nHost: " + entry.Host
	}

	var userAgent string
	if entry.UserAgent != "" {
		userAgent = "\nUserAgent: " + entry.UserAgent
	}

	if len(entry.Trace.Variables) > 0 {
		tagString = "\n       " + tagString
	}

	msg := color.RedBold(entry.Trace.Message)
	output := fmt.Sprintf("\n%s\n%s%s%s%s%s%s\nError: %s%s\n%s",
		apiString, timeString, deploymentID, requestID, remoteHost, host, userAgent, msg, tagString, strings.Join(trace, "\n"))

	fmt.Fprintln(c.Writer, output)
	return nil
}
