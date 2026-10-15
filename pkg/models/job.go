package models

import "time"

type Job struct {
	ID          string
	ExecutionID string
	NodeID      string
	Port        string
	Data        map[string]any
	RunAt       time.Time
}
