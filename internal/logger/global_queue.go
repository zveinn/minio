package logger

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	cbor "github.com/fxamacker/cbor/v2"
	jsoniter "github.com/json-iterator/go"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/logger/target/types"
	"github.com/minio/pkg/v3/logger/message/audit"
	xlog "github.com/minio/pkg/v3/logger/message/log"
	"github.com/valyala/bytebufferpool"
)

const (
	auditGlobalQueueName  = "global_audit_event_queue"
	systemGlobalQueueName = "global_system_event_queue"

	auditQueueConfigDir               = "dir"
	auditQueueConfigDirMaxSize        = "dir_max_size"
	auditQueueConfigDirMaxSizeDefault = 1000
	auditQueueConfigDirMinSize        = 100

	auditQueueConfigMaxDuration        = "max_duration"
	auditQueueConfigMaxDurationDefault = "10s"
)

var (
	httpLoggerExtension = ".http.log"
	httpLoggerPrefix    = "http"

	TargetDefaultMaxWorkers     = 4
	DefaultGlobalEventQueueSize = 500_000
	TargetQueueSize             = 100
	TargetQueueRetryCap         = 80
	defaultMaxFileEntries       = 10000

	GlobalAuditLogger  GlobalQueue[audit.Entry]
	GlobalSTDOutLogger GlobalQueue[xlog.Entry]
	GlobalTestLogger   GlobalQueue[xlog.Entry]
)

type GlobalQueueConfig struct {
	name string

	dir            string
	dirMaxSize     int
	dirMaxDuration time.Duration
}

type GlobalQueue[I any] struct {
	// This channel receives interfaces and
	// distributes them to all Targets
	Ch chan *I

	// Currently active targets which will
	// receive copies of entry batches
	Targets     []*QueueTarget[I]
	TargetCount atomic.Int32
	TargetLock  sync.Mutex

	Config atomic.Pointer[GlobalQueueConfig]

	// Disk management
	maxFileEntries int
	fileIndex      atomic.Int32
	Files          [math.MaxUint16]atomic.Pointer[VirtualFile[I]]
	sizeOnDisk     atomic.Uint64
	Init           func()
}

type VirtualFile[I any] struct {
	diskDir    string
	diskName   string
	Saved      time.Time
	Created    time.Time
	entryCount int

	Entries     []*I
	TargetCount atomic.Int32
}

type queueTargetConfig struct {
	enabled          bool
	batchSize        int
	batchPayloadSize int
	maxRetry         int
	retryInterval    time.Duration
	endpoint         string
	name             string
	targetType       types.TargetType
	encoderType      types.EncoderType
	maxWorkers       int64
	writer           io.Writer
	interfaceWriter  InterfaceWriter
}

type QueueTarget[I any] struct {
	name    string
	enabled atomic.Bool

	workerCount   int64
	maxWorkers    int64
	workerStarted time.Time
	lastActivity  time.Time

	Ch chan *VirtualFile[I]

	runtime atomic.Pointer[queueRuntime]

	TotalMessages  int64
	TotalRequests  int64
	FailedMessages int64
	FailedRequests int64

	cancel context.CancelFunc
	ctx    context.Context
}

type queueRuntime struct {
	Type        types.TargetType
	EncoderType types.EncoderType
	Endpoint    string
	BatchSize   int

	// New
	BatchPayloadSize int
	MaxRetry         int
	RetryInterval    time.Duration

	Writer          io.Writer
	InterfaceWriter InterfaceWriter
}

type InterfaceWriter interface {
	Write(d interface{}) (err error)
}

// Help template for logger http and audit
var AuditEventQueueHelp = config.HelpKVS{
	config.HelpKV{
		Key:         auditQueueConfigDir,
		Description: "The directory where audit events are stored. Note: setting a directory will turn this feature on.",
		Optional:    true,
		Type:        "string",
	},
	config.HelpKV{
		Key:         auditQueueConfigDirMaxSize,
		Description: fmt.Sprintf("The maximum size of the queue direcory in Megabytes. Default: %d", auditQueueConfigDirMaxSizeDefault),
		Optional:    true,
		Type:        "number",
	},
	config.HelpKV{
		Key:         auditQueueConfigMaxDuration,
		Description: fmt.Sprintf("Maximum audit event queue file duration in seconds, max_files takes precedence over max_time. Default: %s", auditQueueConfigMaxDurationDefault),
		Optional:    true,
		Type:        "duration",
	},
}

var DefaultAuditEventQueueKVs = config.KVS{
	config.KV{
		Key:   auditQueueConfigDir,
		Value: "",
	},
	config.KV{
		Key:   auditQueueConfigDirMaxSize,
		Value: strconv.Itoa(auditQueueConfigDirMaxSizeDefault),
	},
	config.KV{
		Key:   auditQueueConfigMaxDuration,
		Value: auditQueueConfigMaxDurationDefault,
	},
}

func InitializeGlobalEventQueues(ctx context.Context) error {
	if runtime.NumCPU() > TargetDefaultMaxWorkers {
		TargetDefaultMaxWorkers = runtime.NumCPU() / 3
	}

	GlobalAuditLogger.Ch = make(chan *audit.Entry, DefaultGlobalEventQueueSize)
	GlobalAuditLogger.maxFileEntries = defaultMaxFileEntries
	GlobalAuditLogger.Targets = make([]*QueueTarget[audit.Entry], 0)

	GlobalSTDOutLogger.Ch = make(chan *xlog.Entry, DefaultGlobalEventQueueSize)
	GlobalSTDOutLogger.maxFileEntries = defaultMaxFileEntries
	GlobalSTDOutLogger.Targets = make([]*QueueTarget[xlog.Entry], 0)

	GlobalSTDOutLogger.UpdateConfig(&GlobalQueueConfig{
		name:           systemGlobalQueueName,
		dir:            "",
		dirMaxDuration: 0,
		dirMaxSize:     0,
	})
	GlobalSTDOutLogger.Start(ctx)
	return nil
}

func (g *GlobalQueue[I]) Start(ctx context.Context) {
	go g.LoadFilesFromDisk(ctx)
	if g.Init == nil {
		g.Init = sync.OnceFunc(func() {
			go g.QueueManager(ctx)
			go g.VirtualFileManager(ctx)
		})
	}
	g.Init()
}

func (g *GlobalQueue[I]) logOnceIf(ctx context.Context, err error, tag string) {
	c := g.Config.Load()
	logOnce.logOnceIf(
		ctx,
		"event_queue",
		err,
		c.name+"_global_event_queue_"+tag)
}

func (t *QueueTarget[I]) logOnceIf(ctx context.Context, err error, tag string) {
	logOnce.logOnceIf(
		ctx,
		"event_queue",
		err,
		t.name+"_global_event_target"+tag)
}

func ValidateAuditEventQueueSybSystemConfig(ctx context.Context, s config.Config) (err error) {
	fmt.Println(s)
	fmt.Println("TODO: AUDIT CONFIG VALIDATION")
	return
}

func ConfigureAndRunAuditEventQueueSubSysteme(ctx context.Context, s config.Config) (err error) {
	kvs := s[config.AuditEventQueueSubSys][config.Default]
	c := &GlobalQueueConfig{
		name: auditGlobalQueueName,
	}

	dir := getCfgVal(
		EnvAuditQueueDir,
		auditQueueConfigDir,
		kvs.Get(auditQueueConfigDir))

	if !strings.HasSuffix(dir, string(os.PathSeparator)) {
		dir += string(os.PathSeparator)
	}

	c.dir = dir

	dirMaxSize := getCfgVal(
		EnvAuditQueueDirMaxSize,
		auditQueueConfigDirMaxSize,
		kvs.Get(auditQueueConfigDirMaxSize))
	dirMaxSizeVal, err := strconv.Atoi(dirMaxSize)
	if err != nil {
		return err
	}
	if dirMaxSizeVal < auditQueueConfigDirMinSize {
		return fmt.Errorf("You should allocate at least %d megabytes to the global event queue directory", auditQueueConfigDirMinSize)
	}

	c.dirMaxSize = dirMaxSizeVal

	maxduration := getCfgVal(
		EnvAuditQueueMaxDuration,
		auditQueueConfigMaxDuration,
		kvs.Get(auditQueueConfigMaxDuration))
	maxDurationVal, err := time.ParseDuration(maxduration)
	if err != nil {
		return err
	}

	if maxDurationVal == 0 {
		time.ParseDuration(auditQueueConfigMaxDurationDefault)
	}

	c.dirMaxDuration = maxDurationVal

	fmt.Println("GLOBAL AUDIT CONFIG:", c)

	GlobalAuditLogger.UpdateConfig(c)
	GlobalAuditLogger.Start(ctx)

	return
}

func (g *GlobalQueue[_]) QueueSize() int {
	return len(g.Ch)
}

func (GQ *GlobalQueue[I]) UpdateConfig(c *GlobalQueueConfig) {
	GQ.Config.Store(c)
}

func (g *GlobalQueue[I]) QueueManager(ctx context.Context) {
	config := g.Config.Load()
	defer func() {
		r := recover()
		if r != nil {
			g.logOnceIf(ctx, fmt.Errorf("%s", r), "panic")
		}

		if ctx.Err() == nil {
			time.Sleep(100 * time.Millisecond)
			g.logOnceIf(ctx, fmt.Errorf("global event queue restarting"), "restart")
			go g.QueueManager(ctx)
		} else {
			g.logOnceIf(ctx, fmt.Errorf("global event queue restarting"), "exit")
		}
	}()

	vf, index := g.nextVirtualFile(ctx)
	var ok bool

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {

		if ctx.Err() != nil {
			return
		}

		select {
		case _ = <-ctx.Done():
			return
		case _ = <-ticker.C:
			if vf.entryCount == 0 {
				fmt.Println("no entries .. discarding", config.name, index, time.Now().Format("15:04:05.000"))
				g.discardFile(index)
				vf, index = g.nextVirtualFile(ctx)
				continue
			}
		case vf.Entries[vf.entryCount], ok = <-g.Ch:
			// fmt.Println("+ENTRY(", config.name, ") targets:", len(g.Targets))
			if !ok {
				return
			}
			vf.entryCount++
			if vf.entryCount < len(vf.Entries)-1 {
				continue
			}
		}

		if len(g.Targets) == 0 {
			fmt.Println("no targets .. discarding", config.name)
			g.discardFile(index)
			vf, index = g.nextVirtualFile(ctx)
			continue
		}

		config = g.Config.Load()
		_ = g.writeToDisk(ctx, vf, config.dir)
		g.sendVirtualFileToAllTargets(ctx, vf)
		vf, index = g.nextVirtualFile(ctx)
	}
}

func (g *GlobalQueue[I]) sendVirtualFileToAllTargets(ctx context.Context, vf *VirtualFile[I]) {
	for _, t := range g.Targets {
		if t.ctx.Err() != nil {
			fmt.Println("global: target ctx done")
			continue
		}

		if !t.enabled.Load() {
			continue
		}

		select {
		case t.Ch <- vf:
			vf.TargetCount.Add(1)
			atomic.AddInt64(&t.TotalMessages, int64(vf.entryCount))
			fmt.Println("global: sending to > ", t.name)
			t.lastActivity = time.Now()
		default:
			atomic.AddInt64(&t.FailedMessages, int64(vf.entryCount))
			g.logOnceIf(ctx, fmt.Errorf("target %s queue is full, dropping entry", t.name), "queue_full")

		}
	}
}

func (q *QueueTarget[_]) Close() {
	q.cancel()
}

func (g *GlobalQueue[_]) RemoveTarget(name string) {
	g.TargetLock.Lock()
	defer g.TargetLock.Unlock()

	for i := range g.Targets {
		if g.Targets[i].name == name {
			g.Targets[i].Close()
			g.Targets = slices.Delete(g.Targets, i, i+1)
			g.TargetCount.Add(-1)
		}
	}

	return
}

func (g *GlobalQueue[I]) FindTarget(name string) *QueueTarget[I] {
	for i := range g.Targets {
		if g.Targets[i].name == name {
			return g.Targets[i]
		}
	}

	return nil
}

func (t *QueueTarget[I]) UpdateTarget(nt *QueueTarget[I]) {
	t.runtime.Store(nt.runtime.Load())
}

func (t *QueueTarget[I]) startInterfaceWriterQueue(ctx context.Context, isPrimary bool) {
	var shouldRestart bool = true
	defer func() {
		r := recover()
		if r != nil {
			log.Println(r, string(debug.Stack()))
		}
		if isPrimary && shouldRestart {
			go t.startInterfaceWriterQueue(ctx, isPrimary)
		}
	}()

	stats := new(types.TargetStats)
	runtime := t.runtime.Load()
	var vf *VirtualFile[I]
	var retries int
	var err error
	var ok bool

	for {
		runtime = t.runtime.Load()
		retries = 0

		select {
		case vf, ok = <-t.Ch:
			if !ok {
				return
			}
		case _ = <-ctx.Done():
			shouldRestart = false
			return
		case _ = <-t.ctx.Done():
			shouldRestart = false
			return
		}

		for i := 0; i < vf.entryCount; i++ {
		reSend:
			runtime = t.runtime.Load()
			err = runtime.InterfaceWriter.Write(vf.Entries[i])
			if ctx.Err() != nil || t.ctx.Err() != nil {
				return
			}

			runtime = t.runtime.Load()

			if err != nil {
				if retries >= runtime.MaxRetry {
					atomic.AddInt64(&t.FailedMessages, 1)
					continue
				}

				if len(t.Ch) < TargetQueueRetryCap {
					retries++

					if runtime.RetryInterval != 0 {
						time.Sleep(runtime.RetryInterval)
					}

					goto reSend
				}

				stats.FailedMessages++
			}
		}

		vf.TargetCount.Add(-1)

	}
}

func (t *QueueTarget[I]) startIOWriterQueue(ctx context.Context, isPrimary bool) {
	if t.workerCount > t.maxWorkers {
		return
	}
	atomic.AddInt64(&t.workerCount, 1)
	defer atomic.AddInt64(&t.workerCount, -1)

	var shouldRestart bool = true
	defer func() {
		r := recover()
		if r != nil {
			log.Println(r, string(debug.Stack()))
		}
		if isPrimary && shouldRestart {
			go t.startIOWriterQueue(ctx, isPrimary)
		}
	}()

	runtime := t.runtime.Load()
	var prevEncoder string
	loadRuntime := func() {
		prevEncoder = string(runtime.EncoderType)
		runtime = t.runtime.Load()
	}

	buff := bytebufferpool.Get()
	defer bytebufferpool.Put(buff)
	tmpBuff := bytebufferpool.Get()
	defer bytebufferpool.Put(tmpBuff)

	encode := func(interface{}) error { return nil }

	encoderChanged := func() (ok bool) {
		if prevEncoder == string(runtime.EncoderType) {
			return false
		}
		// fmt.Println("CHANGING ENCODERS: ", prevEncoder, runtime.EncoderType)
		prevEncoder = string(runtime.EncoderType)
		switch runtime.EncoderType {
		case types.EncoderCBOR:
			encode = cbor.NewEncoder(tmpBuff).Encode
		case types.EncoderJSON:
			encode = jsoniter.ConfigCompatibleWithStandardLibrary.NewEncoder(tmpBuff).Encode
		default:
			encode = jsoniter.ConfigCompatibleWithStandardLibrary.NewEncoder(tmpBuff).Encode
		}

		return true
	}
	encoderChanged()

	stats := new(types.TargetStats)
	var vf *VirtualFile[I]
	var currentEntry int
	var retries int
	var err error
	var ok bool
	var collectStats bool

	for {
		loadRuntime()

		currentEntry = 0
		buff.Reset()
		retries = 0
		collectStats = true

		if !isPrimary {
			if len(t.Ch) < cap(t.Ch)/3 {
				if time.Since(t.workerStarted).Seconds() > 30 {
					shouldRestart = false
					return
				}
			}
		}

		select {
		case vf, ok = <-t.Ch:
			if !ok {
				return
			}
			fmt.Println("+VF(target)")
		case _ = <-ctx.Done():
			fmt.Println("+DONE(global+target)")
			shouldRestart = false
			return
		case _ = <-t.ctx.Done():
			fmt.Println("+DONE(target)")
			shouldRestart = false
			return
		}

		bufferCount := 0
	reEncode:

		for i := currentEntry; i < vf.entryCount; i++ {

			tmpBuff.Reset()
			bufferCount++
			if err := encode(vf.Entries[i]); err != nil {
				t.logOnceIf(ctx, err, "encoding")
				if collectStats {
					stats.FailedMessages++
				}
				continue
			} else if buff.Len()+tmpBuff.Len() < runtime.BatchPayloadSize {
				buff.Write(tmpBuff.Bytes())
				if i < vf.entryCount-1 {
					continue
				}
			}

		reSend:
			stats.TotalRequests++
			_, err = runtime.Writer.Write(buff.Bytes())

			if ctx.Err() != nil || t.ctx.Err() != nil {
				return
			}

			if isPrimary && t.maxWorkers > 1 {
				if len(t.Ch) > cap(t.Ch)/3 {
					if time.Since(t.workerStarted).Milliseconds() > 10 {
						count := atomic.LoadInt64(&t.workerCount)
						if count < t.maxWorkers {
							t.workerStarted = time.Now()
							go t.startIOWriterQueue(ctx, false)
						}
					}
				}
			}

			if err == nil {
				buff.Reset()
				collectStats = true
				bufferCount = 0

			} else {
				t.logOnceIf(ctx, err, "io_write")
				stats.FailedRequests++

				if len(t.Ch) < TargetQueueRetryCap {
					if retries < runtime.MaxRetry {
						retries++

						if runtime.RetryInterval != 0 {
							time.Sleep(runtime.RetryInterval)
						}

						loadRuntime()
						if encoderChanged() {

							currentEntry = i - (bufferCount - 1)
							collectStats = false
							bufferCount = 0

							buff.Reset()
							goto reEncode
						}

						collectStats = false
						goto reSend
					}
				}

				stats.FailedMessages += int64(bufferCount)
			}

		}

		vf.TargetCount.Add(-1)

		if stats != nil {
			atomic.AddInt64(&t.FailedMessages, stats.FailedMessages)
			stats.FailedMessages = 0
			atomic.AddInt64(&t.TotalRequests, stats.TotalRequests)
			stats.TotalRequests = 0
			atomic.AddInt64(&t.FailedRequests, stats.FailedRequests)
			stats.FailedRequests = 0
			stats = new(types.TargetStats)
		}

	}
}

func (g *GlobalQueue[I]) NewQueueTarget(ctx context.Context, C queueTargetConfig) (err error) {
	if C.maxWorkers == 0 {
		C.maxWorkers = int64(TargetDefaultMaxWorkers)
	}

	var createNew bool
	T := g.FindTarget(C.name)
	if T == nil {
		createNew = true

		T = &QueueTarget[I]{
			maxWorkers:    C.maxWorkers,
			Ch:            make(chan *VirtualFile[I], TargetQueueSize),
			workerStarted: time.Now(),
			lastActivity:  time.Now(),
			name:          C.name,
		}
		T.ctx, T.cancel = context.WithCancel(ctx)
	}

	T.enabled.Store(C.enabled)

	T.runtime.Store(&queueRuntime{
		Type:     C.targetType,
		Endpoint: C.endpoint,

		InterfaceWriter: C.interfaceWriter,
		Writer:          C.writer,

		EncoderType:      C.encoderType,
		BatchSize:        C.batchSize,
		BatchPayloadSize: C.batchPayloadSize,
		RetryInterval:    C.retryInterval,
		MaxRetry:         C.maxRetry,
	})

	if createNew {
		g.TargetLock.Lock()
		defer g.TargetLock.Unlock()

		fmt.Println("TODO .. migrate from an old queue_dir to the new queue")

		g.Targets = append(g.Targets, T)
		g.TargetCount.Add(1)

		runtime := T.runtime.Load()

		if runtime.Writer != nil {
			go T.startIOWriterQueue(ctx, true)
		} else if runtime.InterfaceWriter != nil {
			go T.startInterfaceWriterQueue(ctx, true)
		} else {
			err = fmt.Errorf("no writer set for queue target")
			g.logOnceIf(ctx, err, "missing_writer_interface")
			return
		}

	}

	return
}
