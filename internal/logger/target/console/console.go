package console

import (
	"context"
	"fmt"
	"io"
)

func NewConsoleByteTarget(ctx context.Context, replacementWriter io.Writer) *ConsoleByteTarget {
	return &ConsoleByteTarget{
		ctx:    ctx,
		writer: replacementWriter,
	}
}

type ConsoleByteTarget struct {
	ctx    context.Context
	writer io.Writer
}

func (c *ConsoleByteTarget) Write(data []byte) (int, error) {
	if c.writer != nil {
		return fmt.Fprintln(c.writer, string(data))
	} else {
		return fmt.Println(string(data))
	}
}

// type ConsoleInterfaceTarget struct {
// 	ctx context.Context
// }
//
// func (c *ConsoleInterfaceTarget) Write(data interface{}) (int, error) {
// 	return fmt.Println(data)
// }
//
// type ConsoleBatchTarget struct {
// 	ctx context.Context
// }
//
// func (c *ConsoleBatchTarget) Write(data []interface{}) (total int, err error) {
// 	for _, v := range data {
// 		count, errx := fmt.Println(v)
// 		if errx != nil {
// 			return total, errx
// 		}
// 		total += count
// 	}
// 	return total, err
// }
