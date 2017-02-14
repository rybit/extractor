package parsing

import (
	"testing"

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

	if parsed, dims, ok := ParseLine(raw, fields, tl); assert.True(t, ok) {
		assert.Len(t, dims, 0)
		assert.Len(t, parsed, 2)
		for _, p := range parsed {
			switch p.Label {
			case "pos 1":
				validateFields(t, p, "sandman", "pos 1")
			case "marp":
				validateFields(t, p, 123, "marp")
			default:
				assert.Fail(t, "Found unexpected value")
			}
		}
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
	if parsed, dims, ok := ParseLine(raw, fields, tl); assert.True(t, ok) {
		assert.Len(t, parsed, 1)
		assert.EqualValues(t, "else", parsed[0].Value)
		assert.EqualValues(t, "nothing", parsed[0].Label)
		assert.Len(t, dims, 0)
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
	if parsed, dims, ok := ParseLine(raw, fields, tl); assert.True(t, ok) {
		assert.Len(t, dims, 0)
		assert.Len(t, parsed, 1)
		validateFields(t, parsed[0], "else", "nothing")
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

	raw := "url=https://nothing.else.com/matters"
	if parsed, dims, ok := ParseLine(raw, fields, tl); assert.True(t, ok) {
		assert.Len(t, parsed, 1)
		validateFields(t, parsed[0], "nothing.else.com", "url")
		assert.Len(t, dims, 1)
		assert.EqualValues(t, "https", dims["scheme"])

	}
}

func validate(t *testing.T, def *FieldDef, req bool, pos int, label, ftype FieldType, delim string) {
	assert.EqualValues(t, pos, def.Position, "position  mismatch")
	assert.EqualValues(t, label, def.Label, "label mismatch")
	assert.EqualValues(t, ftype, def.Type, "type mismatch")
	assert.EqualValues(t, req, def.Required, "required  mismatch")
	assert.EqualValues(t, delim, def.Delimiter, "delimiter mismatch")
}

func validateFields(t *testing.T, p ParsedField, value interface{}, label string) {
	assert.EqualValues(t, value, p.Value)
	assert.Equal(t, label, p.Label)
}
