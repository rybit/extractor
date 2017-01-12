package cmd

import (
	"testing"
	"time"

	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/hpcloud/tail"
	"github.com/rybit/extractor/parsing"
	"github.com/rybit/nats_metrics"
	"github.com/stretchr/testify/assert"
)

var tl = logrus.NewEntry(logrus.StandardLogger())

func TestProcessingLines(t *testing.T) {
	start := time.Now()
	seenFirst := 0
	seenSecond := 0
	verify := func(m *metrics.RawMetric) {
		switch m.Name {
		case "first.metric":
			seenFirst++
			assert.Len(t, m.Dims, 1)
			assert.Equal(t, "unicorns", m.Dims["magic"])
			switch m.Value {
			case 45634:
				assert.Equal(t, time.Unix(1, 0).UnixNano(), m.Timestamp.UnixNano())
			case 123:
				assert.Equal(t, time.Unix(2, 0).UnixNano(), m.Timestamp.UnixNano())
			case 987:
				assert.Equal(t, time.Unix(4, 0).UnixNano(), m.Timestamp.UnixNano())
			default:
				assert.Fail(t, fmt.Sprintf("unexpected metric: +%v", m))
			}
		case "second.metric":
			seenSecond++
			assert.EqualValues(t, 1, m.Value)
			assert.True(t, start.Before(m.Timestamp))
			assert.Len(t, m.Dims, 2)

			ts := m.Dims["@timestamp"]
			switch ts {
			case "1":
				assert.Equal(t, "https://mysite.is", m.Dims["domain"])
			case "2":
				assert.Equal(t, "http://mysite.is", m.Dims["domain"])
			case "3":
				assert.Equal(t, "https://yoursite.is", m.Dims["domain"])
			default:
				assert.Fail(t, "unexpected metric", m)
			}
		default:
			assert.FailNow(t, "Unknown metric: "+m.Name)
		}
	}
	assert.NoError(t, metrics.Init(nil, "nowhere"))
	metrics.Trace(verify)

	defs := map[string][]parsing.FieldDef{
		"first.metric": []parsing.FieldDef{
			{
				Label:    "timestamp",
				Position: 0,
				Type:     "timestamp",
			}, {
				Position: 1,
				Type:     "value",
			}, {
				Position: 2,
			},
		},
		"second.metric": []parsing.FieldDef{
			{
				Position: 3,
				Type:     "url",
				Required: true,
			}, {
				Position: 0,
			},
		},
	}
	lines := make(chan *tail.Line)

	complete := make(chan (bool))
	go func() {
		processLines(lines, defs, tl)
		complete <- true
	}()

	lines <- l("@timestamp=1 size=45634 magic=unicorns domain=https://mysite.is/phenomenal")
	lines <- l("@timestamp=2 size=123 magic=unicorns domain=http://mysite.is/the-bomb ")
	lines <- l("@timestamp=3 size=not-a-number magic=unicorns domain=https://yoursite.is/tolerable")
	lines <- l("@timestamp=4 size=987 magic=unicorns")

	close(lines)
	select {
	case <-time.After(2 * time.Second):
		assert.FailNow(t, "Didn't recieve messages in time")
	case <-complete:
	}
	assert.Equal(t, 3, seenFirst)
	assert.Equal(t, 3, seenSecond)
}

func l(txt string) *tail.Line {
	return &tail.Line{
		Err:  nil,
		Text: txt,
		Time: time.Now(),
	}
}
