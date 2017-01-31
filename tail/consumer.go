package tail

import (
	"bufio"
	"io"
	"os"
	"sync"
	"time"

	"syscall"

	"github.com/Sirupsen/logrus"
)

type consumer struct {
	Path         string
	log          *logrus.Entry
	shutdown     chan bool
	shutdownLock sync.Mutex
	isShutdown   bool

	FromEnd  bool
	Follow   bool
	offset   int64
	Duration time.Duration
	Out      chan string
}

func newConsumer(path string, log *logrus.Entry) *consumer {
	return &consumer{
		log:      log,
		Path:     path,
		shutdown: make(chan bool, 1),
		Out:      make(chan string),
		Duration: time.Second,
		Follow:   true,
	}
}

func (c *consumer) consume() {
	if c.FromEnd {
		info, err := os.Stat(c.Path)
		for err != nil {
			c.log.WithError(err).Warn("Failed to stat the file while seeking to the end, will retry in a few")
			time.Sleep(time.Second)
			info, err = os.Stat(c.Path)
		}

		c.offset = info.Size()
		c.log.Debugf("Moved to end of the file: %d", c.offset)
	}

	var lastInode uint64

	for {
		select {
		case <-c.shutdown:
			c.log.Debug("Shutting down")
			c.shutdownLock.Lock()
			c.isShutdown = true
			close(c.Out)
			c.shutdownLock.Unlock()
		case <-time.After(c.Duration):
			c.shutdownLock.Lock()
			if c.isShutdown {
				c.shutdownLock.Unlock()
				return
			}
			c.shutdownLock.Unlock()

			info, err := os.Stat(c.Path)
			if err != nil {
				c.log.WithError(err).Warn("Failed to stat the file, will retry in a few")
				continue
			}

			if info.Size() < c.offset {
				c.log.Info("File rotation detected by decreasing size - adjusting seek to the beginning")
				c.offset = 0
			}

			if stats, ok := info.Sys().(*syscall.Stat_t); ok {
				if lastInode != stats.Ino {
					if lastInode != 0 {
						c.offset = 0
						c.log.Info("File rotation detected by inode change, adjusting to the beginning")
					}

					lastInode = stats.Ino
				}
			}

			// open the file
			f, err := os.Open(c.Path)
			if err != nil {
				c.log.WithError(err).Warn("Failed to open the file, will retry in a few")
				continue
			}

			if _, err := f.Seek(c.offset, io.SeekStart); err != nil {
				c.log.Warnf("Failed to seek to %d, going to go to the beginning", c.offset)
				c.offset = 0
				continue
			}

			c.log.Debugf("Starting to scan file from offset: %d", c.offset)
			scanner := bufio.NewScanner(f)
			linesScanned := 0
			for scanner.Scan() {
				c.shutdownLock.Lock()
				if c.isShutdown {
					return
				} else {
					c.Out <- scanner.Text()
					linesScanned++
				}
				c.shutdownLock.Unlock()
			}
			c.log.Debugf("Finished scanning file %d lines", linesScanned)

			// now store the offset again
			off, err := f.Seek(0, io.SeekCurrent)
			if err != nil {
				c.log.Warn("Failed to update the offset of the file")
				off = 0
			}

			c.offset = off

			if !c.Follow {
				c.shutdown <- true
			}
		}
	}
}
