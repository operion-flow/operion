package models

import "time"

type EventFormat string

const (
	EventFormatJSON     EventFormat = "json"
	EventFormatAvro     EventFormat = "avro"
	EventFormatProtobuf EventFormat = "protobuf"
)

type TenantSource string

const (
	TenantFromPayload TenantSource = "payload"
	TenantFromHeader  TenantSource = "header"
)

type EventDefinition struct {
	Key         string         `json:"key"`
	Name        string         `json:"name"             validate:"required"`
	Description string         `json:"description"`
	Topic       string         `json:"topic"            validate:"required"`
	Format      EventFormat    `json:"format"           validate:"required,oneof=json avro protobuf"`
	Schema      map[string]any `json:"schema,omitempty"`
	TenantFrom  TenantSource   `json:"tenant_from"      validate:"required,oneof=payload header"`
	TenantKey   string         `json:"tenant_key"       validate:"required"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
}
