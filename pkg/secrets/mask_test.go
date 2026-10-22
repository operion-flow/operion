package secrets_test

import (
	"net/http"
	"testing"

	"github.com/dukex/operion/pkg/secrets"
	"github.com/stretchr/testify/assert"
)

const headersKey = "headers"

func TestMaskReplacesSecretValuesEverywhere(t *testing.T) {
	mask := secrets.NewMask(map[string]string{crmToken: "s3cr3t-value", "hook": "https://hooks.example/s3cr3t-value/more"})

	masked := mask.Data(map[string]any{
		"token":    "Bearer s3cr3t-value",
		"nested":   map[string]any{"list": []any{"https://hooks.example/s3cr3t-value/more", 42.0, true, nil}},
		headersKey: http.Header{"Authorization": {"Bearer s3cr3t-value"}},
		"plain":    "nothing to hide",
	})

	assert.Equal(t, map[string]any{
		"token":    "Bearer ***",
		"nested":   map[string]any{"list": []any{"***", 42.0, true, nil}},
		headersKey: map[string]any{"Authorization": []any{"Bearer ***"}},
		"plain":    "nothing to hide",
	}, masked)
	assert.Equal(t, "failed to call *** with ***", mask.Text("failed to call https://hooks.example/s3cr3t-value/more with s3cr3t-value"))
}

func TestMaskKeepsTypedValuesWithoutSecrets(t *testing.T) {
	mask := secrets.NewMask(map[string]string{crmToken: "s3cr3t-value"})
	headers := http.Header{"Content-Type": {"application/json"}}

	masked := mask.Data(map[string]any{headersKey: headers})

	assert.Equal(t, headers, masked[headersKey])
}

func TestMaskSkipsShortValues(t *testing.T) {
	assert.Nil(t, secrets.NewMask(map[string]string{"pin": "1234567"}))

	var mask *secrets.Mask

	data := map[string]any{"pin": "1234567"}
	assert.Equal(t, data, mask.Data(data))
	assert.Equal(t, "1234567", mask.Text("1234567"))
}

func TestMaskHandlesValuesThatNeedEscaping(t *testing.T) {
	mask := secrets.NewMask(map[string]string{"quoted": `pa"ss<word>`})

	masked := mask.Data(map[string]any{headersKey: http.Header{"X-Key": {`pa"ss<word>`}}})

	assert.Equal(t, map[string]any{headersKey: map[string]any{"X-Key": []any{"***"}}}, masked)
}
