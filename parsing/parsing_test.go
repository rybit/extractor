package parsing

import (
	"testing"

	"time"

	"github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

var tl = logrus.NewEntry(logrus.StandardLogger())

func TestCmdLineParseGood(t *testing.T) {
	if def := ExtractDefinition("!1:hp:bool", "=", tl); assert.NotNil(t, def) {
		validate(t, def, true, 1, "hp", BoolType, "=")
	}

	if def := ExtractDefinition("12:mp", "=", tl); assert.NotNil(t, def) {
		validate(t, def, false, 12, "mp", StringType, "=")
	}

	if def := ExtractDefinition("123:xp:nonsense", "=", tl); assert.NotNil(t, def) {
		validate(t, def, false, 123, "xp", FieldType("nonsense"), "=")
	}

	if def := ExtractDefinition("1", "=", tl); assert.NotNil(t, def) {
		validate(t, def, false, 1, "", StringType, "=")
	}
}

func TestCmdLineExtractBad(t *testing.T) {
	def := ExtractDefinition("nonsense", "=", tl)
	assert.Nil(t, def)

	def = ExtractDefinition("d:should-be-a-number", "=", tl)
	assert.Nil(t, def)

	def = ExtractDefinition("-2:should-be-positive", "=", tl)
	assert.Nil(t, def)
}

func TestParseLineNiceLine(t *testing.T) {
	fields := []FieldDef{
		{
			Position:  1,
			Label:     "pos 1",
			Type:      StringType,
			Delimiter: "=",
		},
		{
			Position:  2,
			Type:      "number",
			Delimiter: ":",
		},
		{
			Position:  4,
			Delimiter: "=",
		},
	}
	raw := "nothing=else enter=sandman marp:123"
	if res, ok := ParseLine(raw, fields, tl); assert.True(t, ok) {
		assert.Len(t, res.Dims, 2)
		assert.Nil(t, res.Timestamp)
		assert.Equal(t, 123, res.Dims["marp"])
		assert.Equal(t, "sandman", res.Dims["pos 1"])
	}
}

func TestParseLineBadDelimiterMissingRequired(t *testing.T) {
	fields := []FieldDef{
		{
			Position:  0,
			Required:  true,
			Delimiter: "=",
		},
	}

	raw := "nothing:else enter=sandman marp:123"
	_, ok := ParseLine(raw, fields, tl)
	assert.False(t, ok)
}

func TestParseLineMissingRequiredTooShort(t *testing.T) {
	fields := []FieldDef{
		{
			Position:  4,
			Required:  true,
			Delimiter: "=",
		},
	}

	raw := "nothing=else enter=sandman marp:123"
	_, ok := ParseLine(raw, fields, tl)
	assert.False(t, ok)
}

func TestParseLineUnknownFieldType(t *testing.T) {
	fields := []FieldDef{{
		Position:  0,
		Delimiter: "=",
		Type:      FieldType("marp"),
	}}
	raw := "nothing=else enter=sandman marp:123"
	if res, ok := ParseLine(raw, fields, tl); assert.True(t, ok) {
		assert.Len(t, res.Dims, 1)
		assert.Nil(t, res.Timestamp)
		assert.Equal(t, "else", res.Dims["nothing"])
	}
}

func TestParseLineBadDelimiter(t *testing.T) {
	fields := []FieldDef{
		{
			Position:  0,
			Delimiter: "=",
		},
		{
			Position:  1,
			Delimiter: "-",
		},
	}
	raw := "nothing=else enter=sandman marp:123"
	if res, ok := ParseLine(raw, fields, tl); assert.True(t, ok) {
		assert.Len(t, res.Dims, 1)
		assert.Nil(t, res.Timestamp)
		assert.Equal(t, "else", res.Dims["nothing"])
	}
}

func TestParseURLLine(t *testing.T) {
	fields := []FieldDef{
		{
			Position:  0,
			Delimiter: "=",
			Type:      "url",
		},
	}

	raw := "url=https://nothing.else/matters"
	if res, ok := ParseLine(raw, fields, tl); assert.True(t, ok) {
		assert.Len(t, res.Dims, 1)
		assert.Nil(t, res.Timestamp)
		assert.Equal(t, "https://nothing.else", res.Dims["url"])
	}
}

func TestParseLineWithTimestampMsec(t *testing.T) {
	fields := []FieldDef{
		{
			Position:      0,
			Type:          "timestamp",
			TimestampType: "msec",
		},
		{
			Position: 1,
		},
	}

	expectedTime := time.Unix(0, 1485904589183*1000000)
	raw := "@timestamp=1485904589183 nothing=else"
	if res, ok := ParseLine(raw, fields, tl); assert.True(t, ok) {
		assert.Len(t, res.Dims, 1)
		assert.Equal(t, expectedTime.UnixNano(), res.Timestamp.UnixNano())
	}
}

func TestParseLineWithTimestampSec(t *testing.T) {
	fields := []FieldDef{
		{
			Position:      0,
			Type:          "timestamp",
			TimestampType: "sec",
		},
		{
			Position: 1,
		},
	}

	expectedTime := time.Unix(1483142458, 0)
	raw := "@timestamp=1483142458 nothing=else"
	if res, ok := ParseLine(raw, fields, tl); assert.True(t, ok) {
		assert.Len(t, res.Dims, 1)
		assert.Equal(t, expectedTime.UnixNano(), res.Timestamp.UnixNano())
	}
}

func TestParseLineWithGoodValue(t *testing.T) {
	fields := []FieldDef{
		{
			Position: 0,
			Type:     "value",
		},
		{
			Position: 1,
		},
	}

	raw := "size=1483142458 nothing=else"
	if res, ok := ParseLine(raw, fields, tl); assert.True(t, ok) {
		assert.Len(t, res.Dims, 1)
		assert.Nil(t, res.Timestamp)
		assert.EqualValues(t, res.Value, 1483142458)
	}
}

func TestParseLineWithBadValue(t *testing.T) {
	fields := []FieldDef{
		{
			Position: 0,
			Type:     "value",
		},
		{
			Position: 1,
		},
	}

	raw := "size=this-is-not-a-number nothing=else"
	_, ok := ParseLine(raw, fields, tl)
	assert.False(t, ok)
}

func validate(t *testing.T, def *FieldDef, req bool, pos int, label, ftype FieldType, delim string) {
	assert.EqualValues(t, pos, def.Position, "position  mismatch")
	assert.EqualValues(t, label, def.Label, "label mismatch")
	assert.EqualValues(t, ftype, def.Type, "type mismatch")
	assert.EqualValues(t, req, def.Required, "required  mismatch")
	assert.EqualValues(t, delim, def.Delimiter, "delimiter mismatch")
}
