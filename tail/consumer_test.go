package tail

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"errors"

	"github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var tl = logrus.WithField("testing", true)

func init() {
	logrus.SetLevel(logrus.DebugLevel)
}

func TestConsumer(t *testing.T) {
	f, err := ioutil.TempFile("", "extractor-testing")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	c := newConsumer(f.Name(), tl)
	linesSeen := 0
	stopped := make(chan bool)
	stages := make(chan bool)

	// start something to read from the lines
	go func() {
		for range c.Out {
			linesSeen++
			if linesSeen == 10 {
				stages <- true
			}
		}
		stopped <- true
	}()

	// start consuming
	go c.consume()

	// write some lines
	for i := 0; i < 10; i++ {
		f.WriteString(fmt.Sprintf("this is a line %d\n", i))
	}

	// pause for effect
	waitFor(stages, 2)
	assert.Equal(t, 10, linesSeen)

	for i := 0; i < 10; i++ {
		f.WriteString(fmt.Sprintf("this is also a line %d\n", i))
	}

	<-time.After(time.Second * 2)
	c.shutdown <- true

	select {
	case <-stopped:
		assert.Equal(t, 20, linesSeen)
	case <-time.After(time.Second * 5):
		assert.Fail(t, "Failed to get messages in time")
	}
}

func TestReadWithTruncate(t *testing.T) {
	f, err := ioutil.TempFile("", "extractor-testing")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	c := newConsumer(f.Name(), tl)

	linesSeen := 0
	stopped := make(chan bool)
	stages := make(chan bool)
	// start something to read from the lines
	go func() {
		for l := range c.Out {
			linesSeen++
			switch linesSeen {
			case 10:
				assert.Equal(t, fmt.Sprintf("this is a line %d", linesSeen-1), l)
				stages <- true
			case 20:
				assert.Equal(t, fmt.Sprintf("this is also a line %d", linesSeen-11), l)
				stages <- true
			}
		}
		stopped <- true
	}()

	// start consuming
	go c.consume()

	// write some lines
	for i := 0; i < 10; i++ {
		f.WriteString(fmt.Sprintf("this is a line %d\n", i))
	}

	require.NoError(t, waitFor(stages, 2))
	f.Truncate(0)

	for i := 0; i < 10; i++ {
		f.WriteString(fmt.Sprintf("this is also a line %d\n", i))
	}

	require.NoError(t, waitFor(stages, 2))
	c.shutdown <- true

	select {
	case <-stopped:
		assert.Equal(t, 20, linesSeen)
	case <-time.After(time.Second * 10):
		assert.Fail(t, "Failed to get messages in time")
	}
}

func TestReadRemoval(t *testing.T) {
	f, err := ioutil.TempFile("", "extractor-testing")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	c := newConsumer(f.Name(), tl)

	linesSeen := 0
	stopped := make(chan bool)
	stages := make(chan bool)
	// start something to read from the lines
	go func() {
		for range c.Out {
			linesSeen++
			if linesSeen == 10 {
				stages <- true
			} else if linesSeen == 20 {
				stages <- true
			}
		}
		stopped <- true
	}()

	// start consuming
	go c.consume()

	// write some lines
	for i := 0; i < 10; i++ {
		f.WriteString(fmt.Sprintf("this is a line %d\n", i))
	}

	require.NoError(t, waitFor(stages, 2))
	err = os.Remove(f.Name())
	require.NoError(t, err)
	f, err = os.Create(f.Name())
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		f.WriteString(fmt.Sprintf("this is also a line %d\n", i))
	}

	require.NoError(t, waitFor(stages, 2))
	c.shutdown <- true

	select {
	case <-stopped:
		assert.Equal(t, 20, linesSeen)
	case <-time.After(time.Second * 5):
		assert.Fail(t, "Failed to get messages in time")
	}
}

func waitFor(c <-chan bool, sec int) error {
	select {
	case <-c:
	case <-time.After(time.Second * time.Duration(sec)):
		return errors.New("Failed to get it done in time")
	}
	return nil
}

//func TestReadWithNoFollow() {
//
//}

func TestReadFromEndOfFile(t *testing.T) {
	f, err := ioutil.TempFile("", "extractor-testing")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	c := newConsumer(f.Name(), tl)
	c.FromEnd = true

	// write some lines
	for i := 0; i < 10; i++ {
		f.WriteString(fmt.Sprintf("this is a line %d\n", i))
	}

	linesSeen := 0
	stopped := make(chan bool)

	// start something to read from the lines
	go func() {
		for range c.Out {
			linesSeen++
		}
		stopped <- true
	}()

	// start consuming
	go c.consume()
	<-time.After(time.Second * 2)

	for i := 0; i < 10; i++ {
		f.WriteString(fmt.Sprintf("this is also a line %d\n", i))
	}

	<-time.After(time.Second * 2)
	c.shutdown <- true

	select {
	case <-stopped:
		assert.Equal(t, 10, linesSeen)
	case <-time.After(time.Second * 5):
		assert.Fail(t, "Failed to get messages in time")
	}
}
