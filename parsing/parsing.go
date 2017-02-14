package parsing

import (
	"net/url"
	"strconv"
	"strings"

	"time"

	"github.com/Sirupsen/logrus"
	"golang.org/x/net/publicsuffix"
)

type FieldType string

const (
	StringType FieldType = "string"
	NumberType FieldType = "number"
	FloatType  FieldType = "float"
	BoolType   FieldType = "bool"
	URLType    FieldType = "url"
)

type FieldDef struct {
	// required
	Position int `mapstructure:"position"`

	// optional
	Type      FieldType `mapstructure:"type"`
	Label     string    `mapstructure:"label"`
	Delimiter string    `mapstructure:"delim"`
	Required  bool      `mapstructure:"required"`
}

type ParsedField struct {
	Value interface{}
	Label string
}

type ParsedLine struct {
	Timestamp *time.Time
	Value     int64
	Dims      map[string]interface{}
}

func ExtractDefinition(raw, delim string, log *logrus.Entry) *FieldDef {
	def := &FieldDef{
		Type:      StringType,
		Delimiter: delim,
	}

	if strings.HasPrefix(raw, "!") {
		def.Required = true
		raw = raw[1:]
	}

	parts := strings.Split(raw, ":")
	if len(parts) < 1 {
		log.Warnf("Failed to parse '%s'. The format is [!]position:label[:type]", raw)
		return nil
	}
	for i, part := range parts {
		switch i {
		case 0:
			pos, err := strconv.Atoi(part)
			if err != nil {
				log.WithError(err).Warnf("Failed to parse '%s' into an int for the position.", part)
				return nil
			}
			if pos < 0 {
				log.Warnf("Can't have a negative position")
				return nil
			}
			def.Position = pos
		case 1:
			def.Label = part
		case 2:
			def.Type = FieldType(part)
		}
	}

	return def
}

func split(raw, delim string) (string, string, bool) {
	if delim == "" {
		delim = "="
	}
	rawParts := strings.SplitN(raw, delim, 2)
	if len(rawParts) != 2 {
		return "", "", false
	}
	return rawParts[0], rawParts[1], true
}

func ParseLine(raw string, fields []FieldDef, log *logrus.Entry) (map[int]ParsedField, map[string]interface{}, bool) {
	parsed := make(map[int]ParsedField)
	extra := make(map[string]interface{})
	parts := strings.Split(raw, " ")

	for _, def := range fields {
		required := def.Required
		if def.Position >= len(parts) {
			if required {
				log.Warnf("Missing required field at position %d, there are only %d entries", def.Position, len(parts))
				return nil, nil, false
			}
			// we don't care about this field
			continue
		}

		key, rawVal, ok := split(parts[def.Position], def.Delimiter)
		if !ok {
			log.Warnf("Failed to split the field '%s' using delimiter '%s'", parts[def.Position], def.Delimiter)
			if required {
				return nil, nil, false
			}
			continue
		}

		// parse the value component
		var val interface{}
		var err error
		switch def.Type {
		case NumberType:
			val, err = strconv.Atoi(rawVal)
		case FloatType:
			val, err = strconv.ParseFloat(rawVal, 64)
		case BoolType:
			val, err = strconv.ParseBool(rawVal)
		case StringType, FieldType(""):
			val = rawVal
		case URLType:
			var scheme, url, tld string
			scheme, url, tld, err = extractDomain(rawVal)
			extra["scheme"] = scheme
			extra["tld"] = tld
			val = url
		default:
			val = rawVal
			log.Warnf("Unknown field type '%s' treating it as a string", def.Type)
		}

		if err != nil {
			log.WithError(err).Warnf("Failed to convert '%s' to a %s", rawVal, def.Type)
			if required {
				return nil, nil, false
			}
		}
		label := key
		if def.Label != "" {
			label = def.Label
		}

		parsed[def.Position] = ParsedField{
			Value: val,
			Label: label,
		}
	}

	return parsed, extra, true
}

func extractDomain(rawURL string) (string, string, string, error) {
	url, err := url.Parse(rawURL)
	if err != nil {
		return "", "", "", err
	}

	tld, _ := publicsuffix.PublicSuffix(url.Host)
	host := url.Host[:len(url.Host)-len(tld)-1]
	return url.Scheme, host, tld, nil
}
