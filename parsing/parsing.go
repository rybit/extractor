package parsing

import (
	"net/url"
	"strconv"
	"strings"

	"fmt"

	"time"

	"github.com/Sirupsen/logrus"
)

type FieldType string

const (
	StringType    FieldType = "string"
	NumberType    FieldType = "number"
	FloatType     FieldType = "float"
	BoolType      FieldType = "bool"
	URLType       FieldType = "url"
	TimestampType FieldType = "timestamp"
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

func ParseLine(raw string, fields []FieldDef, log *logrus.Entry) (time.Time, map[string]interface{}, bool) {
	timestamp := time.Time{}
	dims := make(map[string]interface{})
	parts := strings.Split(raw, " ")

	for _, def := range fields {
		if def.Position > len(parts) {
			if def.Required {
				log.Warnf("Missing required field at position %d, there are only %d entries", def.Position, len(parts))
				return timestamp, nil, false
			}
			// we don't care about this field
			continue
		}

		// break the key:value pair apart
		part := parts[def.Position]
		delim := def.Delimiter
		if delim == "" {
			delim = "="
		}
		rawParts := strings.SplitN(part, delim, 2)
		if len(rawParts) != 2 {
			log.Warnf("Failed to split the field '%s' using delimiter '%s'", part, def.Delimiter)
			if def.Required {
				return timestamp, nil, false
			}
			continue
		}
		key := rawParts[0]
		rawVal := rawParts[1]

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
			val, err = extractDomain(rawVal)
		case TimestampType:
			timestamp, err = extractTime(rawVal)
			if err != nil {
				log.WithError(err).Warnf("Failed to convert '%s' to a %s", rawVal, def.Type)
				if def.Required {
					return timestamp, nil, false
				}
			}
			continue
		default:
			val = rawVal
			log.Warnf("Unknown field type '%s' treating it as a string", def.Type)
		}

		if err != nil {
			log.WithError(err).Warnf("Failed to convert '%s' to a %s", rawVal, def.Type)
			if def.Required {
				return timestamp, nil, false
			}
		} else {
			if def.Label != "" {
				dims[def.Label] = val
			} else {
				dims[key] = val
			}
		}
	}

	return timestamp, dims, true
}

func extractDomain(rawURL string) (string, error) {
	url, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s://%s", url.Scheme, url.Host), nil
}

func extractTime(rawVal string) (time.Time, error) {
	// could be a number
	if num, err := strconv.Atoi(rawVal); err == nil {
		return time.Unix(int64(num), 0), nil
	}

	// try a few formats
	formats := []string{
		time.RFC822Z, time.RFC822, time.RFC1123Z, time.RFC1123,
		time.RFC3339Nano, time.RFC3339, time.RFC850,
		time.ANSIC, time.RubyDate, time.UnixDate,
	}
	for _, layout := range formats {
		if ts, err := time.Parse(layout, rawVal); err == nil {
			return ts, nil
		}
	}

	return time.Time{}, fmt.Errorf("Failed to parse timestamp from '%s'", rawVal)
}
