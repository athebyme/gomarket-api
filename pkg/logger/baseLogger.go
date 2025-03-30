package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

type BaseLogger struct {
	mu     sync.Mutex
	prefix string
	writer io.Writer
}

func NewLogger(writer io.Writer, prefix string) *BaseLogger {
	return &BaseLogger{
		writer: writer,
		prefix: prefix,
	}
}

func (l *BaseLogger) Log(format string, v ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	message := fmt.Sprintf(l.prefix+" "+format, v...)
	if l.writer != nil {
		_, err := l.writer.Write([]byte(message + "\n"))
		if err != nil {
			log.Printf("failed to write to writer: %v", err)
		}
	}
	log.Print(message)
}

func (l *BaseLogger) WithPrefix(extraPrefix string) Logger {
	return &BaseLogger{
		writer: l.writer,
		prefix: l.prefix + " " + extraPrefix,
	}
}

func (l *BaseLogger) SetPrefix(prefix string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.prefix = prefix
}
func (l *BaseLogger) SetWriter(writer io.Writer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.writer = writer
}

func (l *BaseLogger) Write(p []byte) (n int, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.writer == nil {
		return fmt.Fprintln(os.Stderr, "No writer set for logger:", string(p))
	}

	return l.writer.Write(p)
}

func (l *BaseLogger) FatalLog(format string, v ...interface{}) {
	l.Log(format, v...)
	os.Exit(1)
}
func (l *BaseLogger) Close() error {
	if closer, ok := l.writer.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
