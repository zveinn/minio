package logger

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"runtime/debug"
	"sync/atomic"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/minio/internal/logger/target/http"
	"github.com/minio/minio/internal/logger/target/kafka"
	"github.com/minio/minio/internal/logger/target/testlogger"
	"github.com/minio/minio/internal/logger/target/types"
	"github.com/valyala/bytebufferpool"
)

// TODO .. make .env variables ??
var (
	webhookCallTimeout  = 3 * time.Second
	httpLoggerExtension = ".http.log"
	httpLoggerPrefix    = "http"

	BatchFileMetaSuffix = "_meta"
	maxWorkers          = 20
	// maxWorkersWithBatchEvents     = 4
	TargetDefaultMaxWorkers       = 20
	globlQueueBatchSize           = 1000
	globalQueueSize               = 1000000
	BatchManagerStatusChannelSize = 100000
	BatchManagerBatchChannelSize  = 10000
	TargetQueueSize               = 10000
	GlobalAuditLogger             *GlobalBatchQueue
	GlobalSystemLogger            *GlobalBatchQueue
	GlobalTestLogger              *GlobalBatchQueue
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
	Targets []*QueueTarget

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
		"",
		1,
		"test",
		"test",
		types.TargetConsole,
		nil,
		testlogger.T,
		nil,
	)

	T.Start = func(ctx context.Context) {
		go T.launchFanOutWorker(ctx, true)
	}

	currentTarget := GlobalTestLogger.FindTarget("test")
	if currentTarget != nil {
		currentTarget.UpdateTarget(T)
		return
	}

	GlobalTestLogger.NewTarget(ctx, T)

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
	batch := newBatch(globlQueueBatchSize)
	dumpTicker := time.NewTicker(1 * time.Second)
	batchIndex := 0
	var ok bool

	for {

		if ctx.Err() != nil {
			return
		}

		select {
		case _ = <-dumpTicker.C:
			fmt.Println(g.Name, ">", "DUMP: ", batchIndex)
			if batchIndex == 0 {
				continue
			}
			batchIndex = 0
		case batch.entries[batchIndex], ok = <-g.Ch:
			if !ok {
				return
			}

			batchIndex++
			if batchIndex < globlQueueBatchSize {
				continue
			}
			fmt.Println(g.Name, ">", "BATCH:", batchIndex, len(g.Ch), cap(g.Ch))
			batchIndex = 0
		default:
			time.Sleep(100 * time.Microsecond)
			continue
		}

		if g.BatchManagerEnabled {
			g.BatchManager.BatchCh <- batch
		}

		for i := range g.Targets {
			fmt.Println(g.Name, ">", "TO TARGET:", g.Targets[i].name)
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

		batch = newBatch(globlQueueBatchSize)

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
				QueueLength:    int(v.QLength),
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
	runtime atomic.Pointer[queueRuntime]

	// TODO
	// stats types.TargetStats
	TotalMessages  int64
	FailedMessages int64
	QLength        int64
	QCap           int64
	State          int64

	// Strt function
	Start func(context.Context)
}

type queueRuntime struct {
	//
	// ENCODER TYPE ???
	Type      types.TargetType
	Endpoint  string
	BatchSize int

	Writer          io.Writer
	InterfaceWriter InterfaceWriter
	BatchWriter     BatchInterfaceWriter
}

type InterfaceWriter interface {
	Write(d interface{}) (err error)
}

type BatchInterfaceWriter interface {
	Write(d []interface{}) (err error)
}

func (g *GlobalBatchQueue) RemoveTarget(ctx context.Context, t *QueueTarget) {
	// TODO....
	// g.Targets = append(g.Targets, t)
	// t.Start(ctx)
	return
}

func (g *GlobalBatchQueue) NewTarget(ctx context.Context, t *QueueTarget) {
	g.Targets = append(g.Targets, t)
	t.Start(ctx)
	return
}

func (g *GlobalBatchQueue) FindTarget(name string) *QueueTarget {
	for i := range g.Targets {
		if g.Targets[i].name == name {
			return g.Targets[i]
		}
	}

	return nil
}

func (t *QueueTarget) UpdateTarget(nt *QueueTarget) {
	// Will this do an atomic operation inside
	// an atomic operation ???
	t.runtime.Store(nt.runtime.Load())
}

func (t *QueueTarget) launchSingleWorker(ctx context.Context) {
	shouldRestart := true
	defer func() {
		_ = recover()
		if shouldRestart {
			go t.launchSingleWorker(ctx)
		}
	}()

	r := t.runtime.Load()
	var err error
	var batch *Batch
	ok := false

	written := 0
	failed := 0
	lastSendFailed := false

	for {

		if written > 0 {
			atomic.AddInt64(&t.TotalMessages, int64(written))
		}
		if failed > 0 {
			atomic.AddInt64(&t.FailedMessages, int64(failed))
		}
		if lastSendFailed {
			atomic.AddInt64(&t.State, 0)
		} else {
			atomic.AddInt64(&t.State, 1)
		}

		written = 0
		failed = 0
		lastSendFailed = false

		if ctx.Err() != nil {
			shouldRestart = false
			return
		}

		batch, ok = <-t.Ch
		if !ok {
			return
		}

		isError := func(err error) bool {
			if err == nil {
				written++
				lastSendFailed = false
				return false
			}

			failed++

			if errors.Is(err, context.Canceled) {
				return false
			}

			lastSendFailed = true
			time.Sleep(1 * time.Millisecond)
			return true
		}

		r = t.runtime.Load()

		if r.BatchWriter != nil {
		retryBatch:
			err = r.BatchWriter.Write(batch.entries)
			if isError(err) {
				goto retryBatch
			}
		} else {
			for i := range batch.entries {
			retry:
				err = r.InterfaceWriter.Write(batch.entries[i])
				if isError(err) {
					goto retry
				}
			}
		}

	}
}

func (t *QueueTarget) launchFanOutWorker(ctx context.Context, isPrimary bool) {
	shouldRestart := true
	atomic.AddInt64(&t.workerCount, 1)
	defer atomic.AddInt64(&t.workerCount, -1)
	defer func() {
		_ = recover()
		if isPrimary && shouldRestart {
			go t.launchFanOutWorker(ctx, isPrimary)
		}
	}()
	byteBuffer := bytebufferpool.Get()
	defer bytebufferpool.Put(byteBuffer)

	var err error

	var batch *Batch
	ok := false
	var currentBatchCount int
	// inFailureState := false

	// TODO .. pick encoder based on runtime config...
	r := t.runtime.Load()
	encoderBuffer := jsoniter.ConfigCompatibleWithStandardLibrary.NewEncoder(byteBuffer)

	for {
		r = t.runtime.Load()
		// TODO .. change encoder ?
		// etc...
		currentBatchCount = 0

		if ctx.Err() != nil {
			shouldRestart = false
			return
		}

		if !isPrimary && len(t.Ch) < cap(t.Ch)/3 {
			if time.Since(t.workerStarted).Seconds() > 30 {
				return
			}
		}

		batch, ok = <-t.Ch
		if !ok {
			return
		}

		for i, v := range batch.entries {
			if currentBatchCount != r.BatchSize {
				if err := encoderBuffer.Encode(&v); err != nil {
					fmt.Println("ENCODE ERROR:", err)
					// LOG ???
					// atomic.AddInt64(&t.stats.FailedMessages, 1)
				}
				currentBatchCount++
				fmt.Println("batch count")
				if i < len(batch.entries)-1 {
					continue
				}
			}

		retry:

			if isPrimary {
				atomic.AddInt64(&t.QLength, int64(len(t.Ch)))
				atomic.AddInt64(&t.QCap, int64(cap(t.Ch)))

				if len(t.Ch) > cap(t.Ch)/3 {
					if time.Since(t.workerStarted).Milliseconds() > 10 {

						count := atomic.LoadInt64(&t.workerCount)
						state := atomic.LoadInt64(&t.State)
						// We don't want to spawn to many workers if we
						// are in a failure state. It could overwhelm
						// the endpoint when it comes back online.
						if state == 0 && count < 5 {
							fmt.Println("new worker in failure state")
							t.workerStarted = time.Now()
							go t.launchFanOutWorker(ctx, false)
						} else if count < t.maxWorkers {
							fmt.Println("new worker in success state")
							t.workerStarted = time.Now()
							go t.launchFanOutWorker(ctx, false)
						}

					}
				}
			}

			_, err = r.Writer.Write(byteBuffer.Bytes())
			if err == nil {
				GlobalAuditLogger.BatchManager.BatchStateCh <- &batchStateUpdate{
					Target:           t.name,
					BatchUUID:        batch.UUID,
					LastSuccessIndex: uint16(currentBatchCount),
				}

				atomic.AddInt64(&t.TotalMessages, int64(currentBatchCount))

				if isPrimary {
					atomic.AddInt64(&t.State, 1)
				}
				currentBatchCount = 0
			} else {
				currentBatchCount = 0
				if isPrimary {
					atomic.AddInt64(&t.State, 0)
				}

				if errors.Is(err, context.Canceled) {
					return
				}

				// if we are over 80% capacity we start dropping on errors.
				// batches will be replayed by the batch manager.
				if len(t.Ch) > (cap(t.Ch)/10)*8 {
					byteBuffer.Reset()
					atomic.AddInt64(&t.FailedMessages, int64(currentBatchCount))
					continue
				}

				time.Sleep(1 * time.Millisecond)
				goto retry

			}

			byteBuffer.Reset()
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

type Batch struct {
	UUID    string
	entries []interface{}
	// target // last entry index
	sentStatus map[string]uint16
	lastUpdate time.Time
	lastWrite  time.Time
	Created    time.Time
}

type batchStateUpdate struct {
	Target           string
	BatchUUID        string
	LastSuccessIndex uint16
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
	fmt.Println("newBatch")
	return &Batch{
		entries: make([]interface{}, entries),
		Created: time.Now(),
		// created: time.Now(),
	}
}

func newTempBatch() *Batch {
	fmt.Println("newTempBatch")
	return &Batch{
		Created: time.Now(),
		// created: time.Now(),
	}
}

func (bm *DiskBatchManager) SyncMeta() {
	fmt.Println("SyncMeta")
	// uint16 + newline
	uintb := []byte{0, 0, 10}

	for buuid, batch := range bm.Batches {
		isBatchDone := true
		for _, lastSendIndex := range batch.sentStatus {
			if uint16(len(batch.entries)) > lastSendIndex {
				isBatchDone = false
				break
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

		if batch.lastUpdate.Before(batch.lastWrite) {
			continue
		}

		bm.MetaFiles[buuid].Seek(0, 0)

		for targetName, lastSendIndex := range batch.sentStatus {
			bm.MetaFiles[buuid].Write([]byte(targetName))
			binary.BigEndian.PutUint16(uintb[0:2], lastSendIndex)
			bm.MetaFiles[buuid].Write(uintb)
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
	b.sentStatus[s.Target] = s.LastSuccessIndex
	return
}

func (bm *DiskBatchManager) CreateBatch(b *Batch) {
	fmt.Println("CreateBatch")

	cb, ok := bm.Batches[b.UUID]
	if ok {
		b.lastUpdate = cb.lastUpdate
		b.sentStatus = cb.sentStatus
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
			panic(err)
			continue
		}
	}

	err = os.WriteFile(bm.FileDir+b.UUID, byteBuffer.Bytes(), 0o666)
	if err != nil {
		panic(err)
	}

	bm.MetaFiles[b.UUID], err = os.OpenFile(bm.FileDir+b.UUID+BatchFileMetaSuffix, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		panic(err)
	}

	for i := range b.sentStatus {
		// write name of target
		bm.MetaFiles[b.UUID].Write([]byte(i))
		// write uint16 (last sent index) + new line
		bm.MetaFiles[b.UUID].Write([]byte{0, 0, 10})
	}
}

func newQueueTarget(
	QueueDir string,
	BatchSize int,
	endpoint string,
	name string,
	t types.TargetType,
	writer io.Writer,
	interfaceWriter InterfaceWriter,
	batchWriter BatchInterfaceWriter,
) (T *QueueTarget) {
	if BatchSize <= 0 {
		BatchSize = 1
	}

	T = &QueueTarget{
		maxWorkers:    int64(TargetDefaultMaxWorkers),
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

		GlobalAuditLogger.NewTarget(ctx, t)
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

		GlobalSystemLogger.NewTarget(ctx, t)

	}

	return errs
}

func UpdateHTTPAuditTargets(
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

		currentTarget := GlobalAuditLogger.FindTarget(cfg.Name)
		if currentTarget != nil {
			currentTarget.UpdateTarget(t)
			continue
		}

		GlobalAuditLogger.NewTarget(ctx, t)

	}

	return errs
}

func NewHTTPTarget(
	ctx context.Context,
	config *http.Config,
) (T *QueueTarget, err error) {
	httpTarget, err := http.NewHTTPTarget(ctx, config)
	if err != nil {
		return nil, err
	}

	T = newQueueTarget(
		config.QueueDir,
		config.BatchSize,
		config.Endpoint.String(),
		config.Name,
		types.TargetHTTP,
		httpTarget,
		nil,
		nil,
	)

	T.Start = func(ctx context.Context) {
		go T.launchFanOutWorker(ctx, true)
	}

	return
}

func NewConsoleHTTPTarget(
	ctx context.Context,
	isGlobalDistErasure bool,
	globalLocalNodeName string,
	writer io.Writer,
) (L *HTTPConsoleTarget) {
	L = NewConsoleLogger(
		ctx,
		isGlobalDistErasure,
		globalLocalNodeName,
		writer,
	)

	T := newQueueTarget(
		"",
		1,
		"console",
		"console+http",
		types.TargetConsole,
		nil,
		L,
		nil,
	)

	T.Start = func(ctx context.Context) {
		go T.launchSingleWorker(ctx)
	}

	currentTarget := GlobalSystemLogger.FindTarget("consol+pubsub")
	if currentTarget != nil {
		currentTarget.UpdateTarget(T)
		return
	}

	GlobalSystemLogger.NewTarget(ctx, T)
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

	T = newQueueTarget(
		config.QueueDir,
		1,
		"kafka_"+config.Name,
		config.Name,
		types.TargetKafka,
		target,
		nil,
		nil,
	)

	T.Start = func(ctx context.Context) {
		go T.launchFanOutWorker(ctx, true)
	}

	return
}
