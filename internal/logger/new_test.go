package logger

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

type TestWriter struct {
	start         time.Time
	maxDurationMS int64
	count         atomic.Int64
}

func (t *TestWriter) WaitFor() error {
	// for len(q.Ch) > 0 {
	// 	if time.Since(t.start).Milliseconds() > t.maxDurationMS {
	// 		return errors.New("max duration hit during log queue testing")
	// 	} else {
	// 		time.Sleep(100 * time.Microsecond)
	// 	}
	// }
	return nil
}

func (t *TestWriter) SetTotal(total int64) {
}

func (t *TestWriter) Write(data []byte) (int, error) {
	t.count.Add(1)
	return len(data), nil
}

func TestBenchLoggingQueueSingleWorker1000000Entries(t *testing.T) {
	ctx := context.TODO()

	c := &GlobalQueueConfig{
		name: "system",
	}

	var err error
	fmt.Println(c)
	err = StartTestLogQueue(ctx)
	if err != nil {
		t.Error(err)
	}

	tw := new(TestWriter)
	tw.maxDurationMS = 1000

	L := NewConsoleTarget(ctx, tw)
	if L == nil {
		t.Error("unable to start the HTTP+Console target")
	}

	count := 1000000
	for i := 0; i < count; i++ {
		LogIf(ctx, "", errors.New("NEW ERROR2"))
	}

	tw.start = time.Now()
	go GlobalSTDOutLogger.QueueManager(ctx)

	// err = tw.WaitFor(GlobalSystemLogger)
	// if err != nil {
	// 	t.Error(err)
	// }

	fmt.Printf("%d entires in %d MS \n", tw.count.Load(), time.Since(tw.start).Milliseconds())
}
