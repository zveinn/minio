package logger

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	jsoniter "github.com/json-iterator/go"
	console "github.com/minio/minio/internal/logger/target/console"
	"github.com/minio/minio/internal/logger/target/http"
	"github.com/minio/minio/internal/logger/target/kafka"
	"github.com/minio/minio/internal/logger/target/testlogger"
	"github.com/minio/minio/internal/logger/target/types"
	"github.com/minio/minio/internal/store"
	"github.com/valyala/bytebufferpool"
)

// TODO .. make .env variables ??
var (
	webhookCallTimeout  = 3 * time.Second
	httpLoggerExtension = ".http.log"
	httpLoggerPrefix    = "http"

	BatchFileMetaSuffix     = "_meta"
	errorSleepTime          = time.Duration(100 * time.Millisecond)
	maxWorkers              = 20
	TargetDefaultMaxWorkers = 20
	globlQueueBatchSize     = 1000
	TargetQueueSize         = 10000
	GlobalAuditLogger       *GlobalBatchQueue
	GlobalSystemLogger      *GlobalBatchQueue
	GlobalTestLogger        *GlobalBatchQueue
)

type GlobalBatchQueueConfig struct {
	Name                          string
	EnableBatchManager            bool
	BatchManagerDir               string
	BacthManagerChannelSize       int
	BatchmanagerUpdateChannelSize int

	ChannelSize       int
	BatchSize         int
	TargetChannelSize int
	// MaxWorkers      int
}

type GlobalBatchQueue struct {
	Name string

	// This channel receives interfaces and
	// distributes them to all Targets
	Ch chan interface{}

	// Currently active targets which will
	// receive copies of entry batches
	Targets    []*QueueTarget
	TargetLock sync.Mutex

	// When targets are updated or added
	// they will be places on this queue
	// UpdateTargetCh chan *Trgt
	// UpdateTargetCh chan *QueueTarget

	BatchManagerEnabled bool
	BatchManagerDir     string
	BatchManager        *DiskBatchManager
}

func (g *GlobalBatchQueue) TargetCount() int {
	return len(g.Targets)
}

func StartSystemLogQueue(ctx context.Context) (err error) {
	c := &GlobalBatchQueueConfig{
		Name:               "system",
		EnableBatchManager: false,
		ChannelSize:        1000000,
		BatchSize:          1000,
		TargetChannelSize:  1000,
	}

	GlobalSystemLogger, err = newGlobalQueue(c)
	if err != nil {
		return err
	}

	go GlobalSystemLogger.Init(ctx)
	return nil
}

func StartAuditLogQueue(ctx context.Context) (err error) {
	c := &GlobalBatchQueueConfig{
		Name:               "audit",
		EnableBatchManager: false,
		ChannelSize:        1000000,
		BatchSize:          1000,
		TargetChannelSize:  1000,
	}

	GlobalAuditLogger, err = newGlobalQueue(c)
	if err != nil {
		return err
	}

	go GlobalAuditLogger.Init(ctx)
	return nil
}

func StartTestLogQueue(ctx context.Context) (err error) {
	c := &GlobalBatchQueueConfig{
		EnableBatchManager: false,
		ChannelSize:        1000000,
		BatchSize:          1000,
		TargetChannelSize:  1000,
	}

	GlobalTestLogger, err = newGlobalQueue(c)
	if err != nil {
		return err
	}

	go GlobalTestLogger.Init(ctx)

	T := newQueueTarget(
		1,
		"test",
		"test",
		types.TargetConsole,
		1,
		nil,
		testlogger.T,
		nil,
	)

	currentTarget := GlobalTestLogger.FindTarget("test")
	if currentTarget != nil {
		currentTarget.UpdateTarget(T)
		return
	}

	GlobalTestLogger.InitializeTarget(ctx, T)

	return nil
}

func newGlobalQueue(c *GlobalBatchQueueConfig) (GQ *GlobalBatchQueue, err error) {
	GQ = new(GlobalBatchQueue)
	GQ.Name = c.Name
	GQ.Ch = make(chan interface{}, c.ChannelSize)
	GQ.Targets = make([]*QueueTarget, 0)

	GQ.BatchManagerEnabled = c.EnableBatchManager
	if GQ.BatchManagerEnabled {
		GQ.BatchManager = newBatchManager(
			c.BacthManagerChannelSize,
			c.BatchmanagerUpdateChannelSize,
			c.BatchManagerDir,
		)
	}

	return
}

func (g *GlobalBatchQueue) Init(ctx context.Context) {
	defer func() {
		r := recover()
		if r != nil {
			log.Println(r, string(debug.Stack()))
		}

		// if ctx.Err() != nil {
		// 	time.Sleep(1 * time.Second)
		// 	go g.Init(ctx)
		// }
	}()

	var sentToTargets int
	// TODO .. update with config later
	batchSize := globlQueueBatchSize
	batch := newBatch(batchSize)
	dumpTicker := time.NewTicker(1 * time.Second)
	batchIndex := 0
	var ok bool

	for {

		select {
		case _ = <-dumpTicker.C:
			// fmt.Println(g.Name, ">", "DUMP: ", batchIndex)
			if batchIndex == 0 {
				continue
			}
		case batch.entries[batchIndex], ok = <-g.Ch:
			if !ok {
				return
			}

			batch.totalEntries++
			batchIndex++
			if batchIndex < batchSize {
				continue
			}
			// fmt.Println(g.Name, ">", "BATCH:", batchIndex, len(g.Ch), cap(g.Ch))
			// default:
			// time.Sleep(100 * time.Microsecond)
			continue
		}

		if g.BatchManagerEnabled {
			g.BatchManager.BatchCh <- batch
		}

		// We need to save to disk before we exit, otherwise we could
		// losse some audit log records.
		if ctx.Err() != nil {
			return
		}

		for i := range g.Targets {
			// fmt.Println(g.Name, ">", "TO TARGET:", g.Targets[i].name)
			select {
			// TODO.. make sure we are not sending pointer to the variable
			// but a pointer to the underlying batch struct. And make sure
			// that we are not duplicating.
			case g.Targets[i].Ch <- batch:
				sentToTargets++
				g.Targets[i].lastActivity = time.Now()
			default:
			}
		}

		batchIndex = 0
		batch = newBatch(batchSize)

		if sentToTargets == 0 {
			fmt.Println(g.Name, ">", "NONE")
			// LOG TO CONSOLE...
			// maybe add some stats about how many messages
			// have been moved to console
		}

	}
}

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

func (g *GlobalBatchQueue) GetTargetStats(nameFilter string) (stats []QueueTargetStats) {
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
				QueueLength:    len(v.Ch),
			},
		}
		stats = append(stats, s)
	}

	return
}

//	QUEUE TARGET ....
//
// ================================================
// ================================================
// ================================================
// ================================================
// ================================================
// ================================================
// ================================================
// ================================================
// ================================================
// ================================================
// ================================================
// ================================================
type QueueTarget struct {
	name string

	workerCount   int64
	maxWorkers    int64
	workerStarted time.Time
	lastActivity  time.Time

	Ch      chan *Batch
	Bm      *DiskBatchManager
	runtime atomic.Pointer[queueRuntime]

	TotalMessages  int64
	FailedMessages int64
}

type queueRuntime struct {
	Type        types.TargetType
	EncoderType types.EncoderType
	Endpoint    string
	BatchSize   int

	Writer          io.Writer
	InterfaceWriter InterfaceWriter
	BatchWriter     BatchInterfaceWriter
}

type InterfaceWriter interface {
	Write(d interface{}) (err error)
}

type BatchInterfaceWriter interface {
	WriteBatch(d []interface{}) (n int, err error)
}

func (g *GlobalBatchQueue) RemoveTarget(ctx context.Context, t *QueueTarget) {
	g.TargetLock.Lock()
	defer g.TargetLock.Unlock()

	// TODO....

	return
}

func (g *GlobalBatchQueue) InitializeTarget(ctx context.Context, t *QueueTarget) {
	g.TargetLock.Lock()
	defer g.TargetLock.Unlock()

	t.Bm = g.BatchManager

	g.Targets = append(g.Targets, t)

	go t.launchFanOutWorker(ctx, true)
	return
}

func (g *GlobalBatchQueue) FindTarget(name string) *QueueTarget {
	fmt.Println("TARGETS:", g.Targets)
	for i := range g.Targets {
		if g.Targets[i].name == name {
			return g.Targets[i]
		}
	}

	return nil
}

func (t *QueueTarget) UpdateTarget(nt *QueueTarget) {
	t.runtime.Store(nt.runtime.Load())
}

func (t *QueueTarget) shouldExit() bool {
	if len(t.Ch) < cap(t.Ch)/3 {
		if time.Since(t.workerStarted).Seconds() > 30 {
			return true
		}
	}
	return false
}

func (t *QueueTarget) spawnMoreWorkers(ctx context.Context) {
	if len(t.Ch) < cap(t.Ch)/3 {
		return
	}

	if time.Since(t.workerStarted).Milliseconds() < 10 {
		return
	}

	count := atomic.LoadInt64(&t.workerCount)
	state := atomic.LoadInt64(&t.State)

	// We don't want to spawn to many workers if we
	// are in a failure state. It could overwhelm
	// the endpoint when it comes back online.
	if state == 0 && count < 5 && count < t.maxWorkers {

		fmt.Println("new worker in failure state")
		t.workerStarted = time.Now()
		go t.launchFanOutWorker(ctx, false)

	} else if count < t.maxWorkers {

		fmt.Println("new worker in success state")
		t.workerStarted = time.Now()
		go t.launchFanOutWorker(ctx, false)

	}
}

func (b *DiskBatchManager) SendUpdate(targetName string, uuid string, status batchStatus) {
	select {
	case b.BatchStateCh <- &batchStateUpdate{
		Target:    targetName,
		BatchUUID: uuid,
		Status:    status,
	}:
	default:
		// TODO .. error stdout!
	}
}

func (t *QueueTarget) launchFanOutWorker(ctx context.Context, isPrimary bool) {
	atomic.AddInt64(&t.workerCount, 1)
	defer atomic.AddInt64(&t.workerCount, -1)

	var shouldRestart bool = true
	var batch *Batch
	var err error
	var ok bool

	byteBuffer := bytebufferpool.Get()
	defer bytebufferpool.Put(byteBuffer)

	defer func() {
		_ = recover()
		if isPrimary && shouldRestart {
			go t.launchFanOutWorker(ctx, isPrimary)
		}
	}()

	encoderJSON := jsoniter.ConfigCompatibleWithStandardLibrary.NewEncoder(byteBuffer)

	encode := func(data interface{}, r *queueRuntime) (err error) {
		switch r.EncoderType {
		case types.EncoderCBOR:
		// TODO
		case types.EncoderJSON:
			return encoderJSON.Encode(data)
		default:
			return encoderJSON.Encode(data)
		}
		return nil
	}

	r := t.runtime.Load()

	postWriteOpsCheck := func(err error) (success bool, shouldRetry bool) {
		if isPrimary && t.maxWorkers > 1 {
			t.spawnMoreWorkers(ctx)
		}

		if err == nil {
			return true, false
		}

		if errors.Is(err, context.Canceled) {
			return false, false
		}

		// atomic.AddInt64(&t.FailedMessages, int64(r.BatchSize))

		// If the endpoint is offline we slow things down a bit
		// in order to not create to many useless sockets
		// or spam the network with pointless requests
		if len(t.Ch) < (cap(t.Ch)/10)*9 {
			// 	time.Sleep(errorSleepTime)
			return false, true
		}

		return false, false
	}

	var bs batchStatus
	var retry bool
	var success bool

	for {
		byteBuffer.Reset()

		if ctx.Err() != nil {
			shouldRestart = false
			return
		}

		if !isPrimary {
			t.shouldExit()
		}

		r = t.runtime.Load()

		batch, ok = <-t.Ch
		if !ok {
			return
		}

		bs = statusFailed

		atomic.AddInt64(&t.TotalMessages, int64(batch.totalEntries))

		if r.BatchWriter != nil {

		retryBatch:
			_, err = r.BatchWriter.WriteBatch(batch.entries)
			success, retry = postWriteOpsCheck(err)
			if retry {
				goto retryBatch
			}
			if success {
				bs = statusSent
			} else {
				bs = statusFailed
			}

		} else if r.InterfaceWriter != nil {
			for i := range batch.entries {
				if batch.entries[i] == nil {
					break
				}

			retryInterface:
				err = r.InterfaceWriter.Write(batch.entries[i])
				success, retry = postWriteOpsCheck(err)
				if retry {
					goto retryInterface
				}
			}

			if success {
				bs = statusSent
			} else {
				bs = statusFailed
			}

		} else if r.Writer != nil {
			for i := range batch.entries {
				if batch.entries[i] == nil {
					break
				}

				if err := encode(batch.entries[i], r); err != nil {
					// TODO .. LOG THIS TO CONSOLE
					// TODO .. add to Error count
					atomic.AddInt64(&t.FailedMessages, 1)
					continue
				}

				if i != r.BatchSize-1 {
					continue
				}

			retry:
				_, err = r.Writer.Write(byteBuffer.Bytes())
				success, retry = postWriteOpsCheck(err)
				if retry {
					goto retry
				}

				byteBuffer.Reset()
			}

			if success {
				bs = statusSent
			} else {
				bs = statusFailed
			}

		} else {
			//// HUGE FAILURE .. update failure stats...
		}

		// TODO .. recognize a drop because of capacity and log to stdout

		if t.Bm != nil {
			t.Bm.SendUpdate(t.name, batch.UUID, bs)
		}

	}
}

// BATCH MANAGER ....
// ================================================
// ================================================
// ================================================
// ================================================
// ================================================
// ================================================
// ================================================
// ================================================
// ================================================
// ================================================
// ================================================
// ================================================
type DiskBatchManager struct {
	// Batches from the GlobalBatchQueue are
	// placed on this channel. Each batch size
	// is controlled via user configurations.
	BatchCh      chan *Batch
	BatchStateCh chan *batchStateUpdate
	Batches      map[string]*Batch
	MetaFiles    map[string]*os.File
	FileDir      string
}

type batchStatus int

const (
	statusNone batchStatus = iota
	statusSent
	statusFailed
)

type Batch struct {
	UUID         string
	entries      []interface{}
	totalEntries int
	Status       map[string]batchStatus
	lastUpdate   time.Time
	lastSync     time.Time
	Created      time.Time
}

type batchStateUpdate struct {
	Target    string
	BatchUUID string
	Status    batchStatus
}

func newBatchManager(
	batchChSize int,
	stateChSize int,
	baseDir string,
) *DiskBatchManager {
	return &DiskBatchManager{
		BatchCh:      make(chan *Batch, batchChSize),
		BatchStateCh: make(chan *batchStateUpdate, stateChSize),

		Batches:   make(map[string]*Batch),
		MetaFiles: make(map[string]*os.File),
		FileDir:   baseDir + string(os.PathSeparator),
	}
}

func (bm *DiskBatchManager) Start() {
	var b *Batch
	var s *batchStateUpdate
	var ok bool

	syncTicker := time.NewTicker(2 * time.Second)
	cleanupTicker := time.NewTicker(60 * time.Second)

	for {
		select {
		case b, ok = <-bm.BatchCh:
			bm.CreateBatch(b)
		case s, ok = <-bm.BatchStateCh:
			bm.UpdateBatch(s)
		case _ = <-syncTicker.C:
			bm.SyncMeta()
		case _ = <-cleanupTicker.C:
		}
		if !ok {
			// could read, context canceled ?
		}

	}
}

func newBatch(entries int) *Batch {
	// fmt.Println("newBatch")
	return &Batch{
		entries: make([]interface{}, entries),
		Created: time.Now(),
		// created: time.Now(),
	}
}

func newTempBatch() *Batch {
	// fmt.Println("newTempBatch")
	return &Batch{
		Created: time.Now(),
		// created: time.Now(),
	}
}

func (bm *DiskBatchManager) SyncMeta() {
	fmt.Println("SyncMeta")

	for buuid, batch := range bm.Batches {
		isBatchDone := true
		for _, done := range batch.Status {
			if !done {
				isBatchDone = false
			}
		}

		if isBatchDone {
			err := os.Remove(bm.FileDir + buuid + BatchFileMetaSuffix)
			if err != nil {
				panic(err)
			}
			err = os.Remove(bm.FileDir + buuid)
			if err != nil {
				panic(err)
			}
			continue
		}

		if batch.lastUpdate.Before(batch.lastSync) {
			continue
		}

		bm.MetaFiles[buuid].Seek(0, 0)

		for targetName, done := range batch.Status {
			if done {
				bm.MetaFiles[buuid].Write([]byte{1})
			} else {
				bm.MetaFiles[buuid].Write([]byte{0})
			}
			bm.MetaFiles[buuid].Write([]byte(targetName))
		}

	}
}

func (bm *DiskBatchManager) UpdateBatch(s *batchStateUpdate) {
	fmt.Println("UpdateBatch")
	b, ok := bm.Batches[s.BatchUUID]
	if !ok {
		bm.Batches[s.BatchUUID] = newTempBatch()
		b = bm.Batches[s.BatchUUID]
	}
	b.lastUpdate = time.Now()
	b.Status[s.Target] = true
	return
}

func (bm *DiskBatchManager) CreateBatch(b *Batch) {
	fmt.Println("CreateBatch")

	cb, ok := bm.Batches[b.UUID]
	if ok {
		b.lastUpdate = cb.lastUpdate
		b.Status = cb.Status
	}

	bm.Batches[b.UUID] = b

	var err error

	byteBuffer := bytebufferpool.Get()
	encoder := jsoniter.ConfigCompatibleWithStandardLibrary.NewEncoder(byteBuffer)
	defer bytebufferpool.Put(byteBuffer)
	defer byteBuffer.Reset()

	for _, v := range b.entries {
		err = encoder.Encode(v)
		if err != nil {
			// TODO STATS
			panic(err)
		}
	}

	err = os.WriteFile(bm.FileDir+b.UUID, byteBuffer.Bytes(), 0o666)
	if err != nil {
		// GlobalSystemLogger <-
		// TODO ????
		panic(err)
	}

	bm.MetaFiles[b.UUID], err = os.OpenFile(bm.FileDir+b.UUID+BatchFileMetaSuffix, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		// TODO ?????
		panic(err)
	}

	for i := range b.Status {
		// write name of target
		bm.MetaFiles[b.UUID].Write([]byte(i))
		// write uint16 (last sent index) + new line
		bm.MetaFiles[b.UUID].Write([]byte{0, 0, 10})
	}
	bm.MetaFiles[b.UUID].Sync()
}

func newQueueTarget(
	BatchSize int,
	endpoint string,
	name string,
	t types.TargetType,
	maxWorkers int64,
	writer io.Writer,
	interfaceWriter InterfaceWriter,
	batchWriter BatchInterfaceWriter,
) (T *QueueTarget) {
	if BatchSize <= 0 {
		BatchSize = 1
	}

	T = &QueueTarget{
		maxWorkers:    maxWorkers,
		Ch:            make(chan *Batch, TargetQueueSize),
		workerStarted: time.Now(),
		lastActivity:  time.Now(),
		name:          name,
	}

	T.runtime.Store(&queueRuntime{
		Writer:          writer,
		InterfaceWriter: interfaceWriter,
		BatchWriter:     batchWriter,
		BatchSize:       BatchSize,
		Type:            t,
		Endpoint:        endpoint,
	})

	return
}

func UpdateKafkaAuditTargets(
	ctx context.Context,
	cfgs map[string]kafka.Config,
) (errs []error) {
	for _, cfg := range cfgs {
		if !cfg.Enabled {
			continue
		}

		t, err := NewKafkaTarget(ctx, &cfg)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		currentTarget := GlobalAuditLogger.FindTarget(cfg.Name)
		if currentTarget != nil {
			currentTarget.UpdateTarget(t)
			continue
		}

		GlobalAuditLogger.InitializeTarget(ctx, t)
	}

	return errs
}

func UpdateHTTPSystemTargets(
	ctx context.Context,
	cfgs map[string]http.Config,
) (errs []error) {
	for _, cfg := range cfgs {
		if !cfg.Enabled {
			continue
		}

		t, err := NewHTTPTarget(ctx, &cfg)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		currentTarget := GlobalSystemLogger.FindTarget(cfg.Name)
		if currentTarget != nil {
			currentTarget.UpdateTarget(t)
			continue
		}

		GlobalSystemLogger.InitializeTarget(ctx, t)

	}

	return errs
}

func MigrateOldStore(cfg *http.Config) (err error) {
	queueStore := store.NewQueueStore[interface{}](
		filepath.Join(cfg.QueueDir, cfg.Name),
		uint64(cfg.QueueSize),
		httpLoggerExtension,
	)

	if err := queueStore.Open(); err != nil {
		return fmt.Errorf("unable to initialize the queue store of %s webhook: %w", cfg.Name, err)
	}
	return nil
}

func UpdateHTTPAuditTargets(
	ctx context.Context,
	cfgs map[string]http.Config,
) (errs []error) {
	for _, cfg := range cfgs {
		if !cfg.Enabled {
			continue
		}

		var t *QueueTarget
		var err error
		t, err = NewHTTPTarget(ctx, &cfg)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		currentTarget := GlobalAuditLogger.FindTarget(cfg.Name)
		if currentTarget != nil {
			currentTarget.UpdateTarget(t)
			continue
		}

		GlobalAuditLogger.InitializeTarget(ctx, t)

	}

	return errs
}

func NewConsoleTarget(
	ctx context.Context,
	optionalReplacementWriter io.Writer,
) (L *console.StdOutTarget) {
	L = console.NewStdOutTarget(ctx, optionalReplacementWriter)

	T := newQueueTarget(
		1,
		"console",
		"console",
		types.TargetConsole,
		1,
		nil,
		nil,
		L,
	)

	currentTarget := GlobalSystemLogger.FindTarget("consol+bytes")
	if currentTarget != nil {
		currentTarget.UpdateTarget(T)
		return
	}

	GlobalSystemLogger.InitializeTarget(ctx, T)
	return
}

func NewConsolePubSubTarget(
	ctx context.Context,
	isGlobalDistErasure bool,
	globalLocalNodeName string,
	optionalReplacementWriter io.Writer,
) (L *ConsolePubSubTarget) {
	L = NewConsolePubSubLogger(
		ctx,
		isGlobalDistErasure,
		globalLocalNodeName,
		optionalReplacementWriter,
	)

	T := newQueueTarget(
		1,
		"console",
		"console+http",
		types.TargetConsole,
		int64(TargetDefaultMaxWorkers),
		nil,
		L,
		nil,
	)

	currentTarget := GlobalSystemLogger.FindTarget("consol+pubsub")
	if currentTarget != nil {
		currentTarget.UpdateTarget(T)
		return
	}

	GlobalSystemLogger.InitializeTarget(ctx, T)
	return
}

func NewHTTPTarget(
	ctx context.Context,
	config *http.Config,
) (T *QueueTarget, err error) {
	httpTarget, err := http.NewHTTPTarget(ctx, config)
	if err != nil {
		return nil, err
	}

	// TODO .. deal with old queueDIR
	T = newQueueTarget(
		config.BatchSize,
		config.Endpoint.String(),
		config.Name,
		types.TargetHTTP,
		int64(TargetDefaultMaxWorkers),
		nil,
		nil,
		httpTarget,
	)

	return
}

func NewKafkaTarget(
	ctx context.Context,
	config *kafka.Config,
) (T *QueueTarget, err error) {
	target, err := kafka.NewKafaTarget(ctx, config)
	if err != nil {
		return nil, err
	}

	// TODO .. deal with old queue dir

	T = newQueueTarget(
		1,
		"kafka_"+config.Name,
		config.Name,
		types.TargetKafka,
		int64(TargetDefaultMaxWorkers),
		target,
		nil,
		nil,
	)

	return
}
