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
	if _, res, ok := ParseLine(raw, fields, tl); assert.True(t, ok) {
		assert.Len(t, res, 2)
		assert.Equal(t, 123, res["marp"])
		assert.Equal(t, "sandman", res["pos 1"])
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
	_, _, ok := ParseLine(raw, fields, tl)
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
	_, _, ok := ParseLine(raw, fields, tl)
	assert.False(t, ok)
}

func TestParseLineUnknownFieldType(t *testing.T) {
	fields := []FieldDef{{
		Position:  0,
		Delimiter: "=",
		Type:      FieldType("marp"),
	}}
	raw := "nothing=else enter=sandman marp:123"
	if _, res, ok := ParseLine(raw, fields, tl); assert.True(t, ok) {
		assert.Len(t, res, 1)
		assert.Equal(t, "else", res["nothing"])
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
	if _, res, ok := ParseLine(raw, fields, tl); assert.True(t, ok) {
		assert.Len(t, res, 1)
		assert.Equal(t, "else", res["nothing"])
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
	if _, res, ok := ParseLine(raw, fields, tl); assert.True(t, ok) {
		assert.Len(t, res, 1)
		assert.Equal(t, "https://nothing.else", res["url"])
	}
}

func TestParseLineWithTimestamp(t *testing.T) {
	fields := []FieldDef{
		{
			Position: 0,
			Type:     "timestamp",
		},
		{
			Position: 1,
		},
	}

	expectedTime := time.Unix(1483142458, 0)
	raw := "@timestamp=1483142458 nothing=else"
	if ts, res, ok := ParseLine(raw, fields, tl); assert.True(t, ok) {
		assert.Len(t, res, 1)
		assert.Equal(t, expectedTime.UnixNano(), ts.UnixNano())
	}
}

func validate(t *testing.T, def *FieldDef, req bool, pos int, label, ftype FieldType, delim string) {
	assert.EqualValues(t, pos, def.Position, "position  mismatch")
	assert.EqualValues(t, label, def.Label, "label mismatch")
	assert.EqualValues(t, ftype, def.Type, "type mismatch")
	assert.EqualValues(t, req, def.Required, "required  mismatch")
	assert.EqualValues(t, delim, def.Delimiter, "delimiter mismatch")
}
