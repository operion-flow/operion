package secret_test

import (
	"testing"

	"github.com/dukex/operion/pkg/secret"
	"github.com/stretchr/testify/assert"
)

func TestFromEnvironReadsOnlyThePrefixedVariables(t *testing.T) {
	values := secret.FromEnviron([]string{
		"OPERION_SECRET_CRM_TOKEN=s3cr3t",
		"OPERION_SECRET_SLACK_WEBHOOK_URL=https://hooks.slack.com/services/a=b",
		"OPERION_SECRET_=nameless",
		"DATABASE_URL=postgres://operion:operion@db/operion",
		"OPERION_API_TOKEN=platform",
		"MALFORMED",
	})

	assert.Equal(t, map[string]string{
		"crm_token":         "s3cr3t",
		"slack_webhook_url": "https://hooks.slack.com/services/a=b",
	}, values)
}
