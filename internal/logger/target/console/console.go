package console

import (
	"context"
	"fmt"
	"io"
)

func NewStdOutTarget(ctx context.Context, replacementWriter io.Writer) *StdOutTarget {
	return &StdOutTarget{
		ctx:    ctx,
		writer: replacementWriter,
	}
}

type StdOutTarget struct {
	ctx    context.Context
	writer io.Writer
}

func (c *StdOutTarget) Write(data interface{}) (err error) {
	if c.writer != nil {
		_, err = fmt.Fprintln(c.writer, data)
		return
	}
	_, err = fmt.Println(data)
	return
}
