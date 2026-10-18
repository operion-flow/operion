package models

import "time"

type RemoteNodeType struct {
	ID             string         `json:"id"`
	Name           string         `json:"name"             validate:"required"`
	Description    string         `json:"description"`
	URL            string         `json:"url"              validate:"required,http_url"`
	Schema         map[string]any `json:"schema,omitempty"`
	Outputs        []string       `json:"outputs"`
	TimeoutSeconds int            `json:"timeout_seconds"  validate:"gte=0,lte=300"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
}
