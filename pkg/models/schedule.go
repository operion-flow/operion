package models

import "time"

type Schedule struct {
	WorkflowID string
	NodeID     string
	Expression string
	Timezone   string
	NextRunAt  time.Time
}

type Firing struct {
	Execution *ExecutionContext
	Job       Job
	NextRunAt time.Time
}
