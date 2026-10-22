package secret

import "strings"

const EnvPrefix = "OPERION_SECRET_"

// FromEnviron returns the secrets among environ, the KEY=value pairs os.Environ gives: OPERION_SECRET_CRM_TOKEN becomes crm_token.
func FromEnviron(environ []string) map[string]string {
	values := make(map[string]string)

	for _, pair := range environ {
		key, value, ok := strings.Cut(pair, "=")
		if !ok {
			continue
		}

		name, found := strings.CutPrefix(key, EnvPrefix)
		if !found || name == "" {
			continue
		}

		values[strings.ToLower(name)] = value
	}

	return values
}
