package logger

import (
	"context"
	"io"
	"os"

	console "github.com/minio/minio/internal/logger/target/console"
	"github.com/minio/minio/internal/logger/target/http"
	"github.com/minio/minio/internal/logger/target/kafka"
	"github.com/minio/minio/internal/logger/target/types"
	xlog "github.com/minio/pkg/v3/logger/message/log"
)

func NewConsoleTarget(ctx context.Context, optionalReplacementWriter io.Writer) (L *console.StdOutTarget) {
	L = console.NewStdOutTarget(ctx, optionalReplacementWriter)

	GlobalSTDOutLogger.NewQueueTarget(
		ctx,
		queueTargetConfig{
			enabled:          true,
			maxRetry:         1,
			retryInterval:    0,
			batchPayloadSize: 10,
			endpoint:         "console",
			name:             "console",
			targetType:       types.TargetConsole,
			interfaceWriter:  L,
		},
	)

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

	GlobalSTDOutLogger.NewQueueTarget(
		ctx,
		queueTargetConfig{
			enabled:          true,
			maxRetry:         1,
			retryInterval:    0,
			batchPayloadSize: 0,
			endpoint:         "console",
			name:             "console+pubsub",
			targetType:       types.TargetConsole,
			interfaceWriter:  L,
		},
	)

	return
}

func (g *GlobalQueue[I]) NewHTTPQueueTarget(ctx context.Context, config *http.Config) (err error) {
	httpTarget, err := http.NewHTTPTarget(ctx, config)
	if err != nil {
		return err
	}

	g.NewQueueTarget(
		ctx,
		queueTargetConfig{
			enabled:          config.Enabled,
			maxWorkers:       int64(TargetDefaultMaxWorkers),
			maxRetry:         config.MaxRetry,
			retryInterval:    config.RetryIntvl,
			batchPayloadSize: config.BatchPayloadSize,
			batchSize:        config.BatchSize,
			endpoint:         config.Endpoint.String(),
			name:             config.Name,
			targetType:       types.TargetHTTP,
			encoderType:      types.EncoderType(config.Encoding),
			writer:           httpTarget,
		},
	)

	return
}

func (g *GlobalQueue[I]) UpdateHTTPTargets(ctx context.Context, cfgs map[string]http.Config) (errs []error) {
	for _, cfg := range cfgs {

		err := g.NewHTTPQueueTarget(ctx, &cfg)
		if err != nil {
			errs = append(errs, err)
			continue
		}

	}

	return errs
}

func (g *GlobalQueue[I]) NewKafkaTarget(ctx context.Context, config *kafka.Config) (err error) {
	target, err := kafka.NewKafaTarget(ctx, config)
	if err != nil {
		return err
	}

	GlobalAuditLogger.NewQueueTarget(
		ctx,
		queueTargetConfig{
			batchSize:       1,
			endpoint:        "kafka_" + config.Name,
			name:            config.Name,
			targetType:      types.TargetKafka,
			interfaceWriter: target,
		},
	)

	return
}

func (g *GlobalQueue[I]) UpdateKafkaTargets(ctx context.Context, cfgs map[string]kafka.Config) (errs []error) {
	for _, cfg := range cfgs {

		err := g.NewKafkaTarget(ctx, &cfg)
		if err != nil {
			errs = append(errs, err)
			continue
		}

	}

	return errs
}

func StartTestLogQueue(ctx context.Context) (err error) {
	c := &GlobalQueueConfig{}

	GlobalTestLogger.Ch = make(chan *xlog.Entry, 1000)
	GlobalTestLogger.Targets = make([]*QueueTarget[xlog.Entry], 0)
	GlobalTestLogger.UpdateConfig(c)

	go GlobalTestLogger.QueueManager(ctx)

	GlobalTestLogger.NewQueueTarget(
		ctx,
		queueTargetConfig{
			batchSize:   1,
			endpoint:    "test",
			name:        "test",
			targetType:  types.TargetConsole,
			encoderType: types.EncoderNone,
			maxWorkers:  1,
			writer:      os.Stdout,
		},
	)

	return nil
}
