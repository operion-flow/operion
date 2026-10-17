// Package registry provides node factory registration for the registry system.
package registry

import (
	"github.com/dukex/operion/pkg/nodes/conditional"
	"github.com/dukex/operion/pkg/nodes/httprequest"
	"github.com/dukex/operion/pkg/nodes/log"
	switchnode "github.com/dukex/operion/pkg/nodes/switch"
	"github.com/dukex/operion/pkg/nodes/transform"
	"github.com/dukex/operion/pkg/nodes/trigger"
	"github.com/dukex/operion/pkg/nodes/wait"
)

// RegisterDefaultNodes registers all built-in node factories with the registry.
func (r *Registry) RegisterDefaultNodes() {
	// Register HTTP Request node
	r.RegisterNode(httprequest.NewHTTPRequestNodeFactory())

	// Register Transform node
	r.RegisterNode(transform.NewTransformNodeFactory())

	// Register Log node
	r.RegisterNode(log.NewLogNodeFactory())

	// Register Conditional node
	r.RegisterNode(conditional.NewConditionalNodeFactory())

	// Register Switch node
	r.RegisterNode(switchnode.NewSwitchNodeFactory())
	r.RegisterNode(wait.NewWaitFactory())
	r.RegisterNode(wait.NewWaitUntilFactory())

	// Register Trigger nodes
	r.RegisterNode(trigger.NewWebhookTriggerNodeFactory())
	r.RegisterNode(trigger.NewSchedulerTriggerNodeFactory())
	r.RegisterNode(trigger.NewKafkaTriggerNodeFactory())
}
