package secret

import (
	"cmp"
	"encoding/json"
	"slices"
	"strings"
)

const (
	Masked = "***"

	MinMaskedLength = 8
)

// Mask replaces secret values of at least MinMaskedLength characters with Masked.
type Mask struct {
	text    *strings.Replacer
	escaped []string
}

// NewMask returns nil when no value is long enough to mask; a nil Mask leaves everything as it is.
func NewMask(values map[string]string) *Mask {
	var masked []string

	for _, value := range values {
		if len(value) >= MinMaskedLength {
			masked = append(masked, value)
		}
	}

	if len(masked) == 0 {
		return nil
	}

	slices.SortFunc(masked, func(a, b string) int { return cmp.Compare(len(b), len(a)) })

	pairs := make([]string, 0, 2*len(masked))
	escaped := make([]string, 0, len(masked))

	for _, value := range masked {
		pairs = append(pairs, value, Masked)

		encoded, err := json.Marshal(value)
		if err == nil {
			escaped = append(escaped, string(encoded[1:len(encoded)-1]))
		}
	}

	return &Mask{text: strings.NewReplacer(pairs...), escaped: escaped}
}

// Text masks every secret value in s.
func (m *Mask) Text(s string) string {
	if m == nil {
		return s
	}

	return m.text.Replace(s)
}

// Data masks every secret value in the strings of data, through nested maps and lists.
func (m *Mask) Data(data map[string]any) map[string]any {
	if m == nil || data == nil {
		return data
	}

	masked, ok := m.value(data).(map[string]any)
	if !ok {
		return data
	}

	return masked
}

func (m *Mask) value(value any) any {
	switch typed := value.(type) {
	case string:
		return m.text.Replace(typed)
	case map[string]any:
		masked := make(map[string]any, len(typed))
		for key, item := range typed {
			masked[key] = m.value(item)
		}

		return masked
	case []any:
		masked := make([]any, len(typed))
		for i, item := range typed {
			masked[i] = m.value(item)
		}

		return masked
	case nil, bool, float64, float32, int, int32, int64, json.Number:
		return typed
	default:
		return m.generic(typed)
	}
}

func (m *Mask) generic(value any) any {
	raw, err := json.Marshal(value)
	if err != nil || !m.holdsSecret(string(raw)) {
		return value
	}

	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return Masked
	}

	return m.value(decoded)
}

func (m *Mask) holdsSecret(encoded string) bool {
	return slices.ContainsFunc(m.escaped, func(escaped string) bool { return strings.Contains(encoded, escaped) })
}
