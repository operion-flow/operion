package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"time"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/template"
)

const (
	inputPort       = "main"
	successPort     = "success"
	errorPort       = "error"
	defaultTimeout  = 30
	maxResponseSize = 1 << 20
)

func Normalize(nodeType *models.RemoteNodeType) {
	if len(nodeType.Outputs) == 0 {
		nodeType.Outputs = []string{successPort}
	}

	if !slices.Contains(nodeType.Outputs, errorPort) {
		nodeType.Outputs = append(nodeType.Outputs, errorPort)
	}

	if nodeType.TimeoutSeconds == 0 {
		nodeType.TimeoutSeconds = defaultTimeout
	}
}

type Node struct {
	nodeType models.RemoteNodeType
	id       string
	config   map[string]any
	client   *http.Client
}

func New(nodeType models.RemoteNodeType, id string, config map[string]any, client *http.Client) *Node {
	Normalize(&nodeType)

	return &Node{nodeType: nodeType, id: id, config: config, client: client}
}

func (n *Node) ID() string {
	return n.id
}

func (n *Node) Type() string {
	return n.nodeType.ID
}

func (n *Node) InputPorts() []models.InputPort {
	return []models.InputPort{{Port: models.Port{ID: models.MakePortID(n.id, inputPort), NodeID: n.id, Name: inputPort}}}
}

func (n *Node) OutputPorts() []models.OutputPort {
	ports := make([]models.OutputPort, 0, len(n.nodeType.Outputs))
	for _, name := range n.nodeType.Outputs {
		ports = append(ports, models.OutputPort{Port: models.Port{ID: models.MakePortID(n.id, name), NodeID: n.id, Name: name}})
	}

	return ports
}

func (n *Node) Validate(config map[string]any) error {
	required, _ := n.nodeType.Schema["required"].([]any)
	for _, field := range required {
		name, _ := field.(string)
		if _, ok := config[name]; !ok {
			return fmt.Errorf("missing required config field %q", name)
		}
	}

	return nil
}

type request struct {
	ExecutionID string         `json:"execution_id"`
	WorkflowID  string         `json:"workflow_id"`
	NodeID      string         `json:"node_id"`
	Config      any            `json:"config"`
	Input       map[string]any `json:"input"`
	TriggerData map[string]any `json:"trigger_data"`
	NodeResults map[string]any `json:"node_results"`
	Variables   map[string]any `json:"variables"`
}

type response struct {
	Port string         `json:"port"`
	Data map[string]any `json:"data"`
}

func (n *Node) Execute(execution models.ExecutionContext, inputs map[string]models.NodeResult) (map[string]models.NodeResult, error) {
	config, err := render(n.config, &execution)
	if err != nil {
		return n.fail(fmt.Errorf("config template: %w", err)), nil
	}

	nodeResults := make(map[string]any, len(execution.NodeResults))
	for nodeID, result := range execution.NodeResults {
		nodeResults[nodeID] = result.Data
	}

	body, err := json.Marshal(request{
		ExecutionID: execution.ID,
		WorkflowID:  execution.WorkflowID,
		NodeID:      n.id,
		Config:      config,
		Input:       inputs[inputPort].Data,
		TriggerData: execution.TriggerData,
		NodeResults: nodeResults,
		Variables:   execution.Variables,
	})
	if err != nil {
		return n.fail(fmt.Errorf("encode request: %w", err)), nil
	}

	answer, err := n.call(body)
	if err != nil {
		return n.fail(err), nil
	}

	if !slices.Contains(n.nodeType.Outputs, answer.Port) {
		return n.fail(fmt.Errorf("remote node answered unknown port %q", answer.Port)), nil
	}

	status := models.NodeStatusSuccess
	if answer.Port == errorPort {
		status = models.NodeStatusError
	}

	return map[string]models.NodeResult{answer.Port: {NodeID: n.id, Data: answer.Data, Status: string(status)}}, nil
}

func (n *Node) call(body []byte) (*response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(n.nodeType.TimeoutSeconds)*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.nodeType.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := n.client.Do(req)
	if err != nil {
		return nil, err
	}

	defer func() { _ = resp.Body.Close() }()

	payload, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseSize))
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("remote node answered status %d: %s", resp.StatusCode, payload)
	}

	var answer response
	if err := json.Unmarshal(payload, &answer); err != nil {
		return nil, fmt.Errorf("remote node answered invalid JSON: %w", err)
	}

	return &answer, nil
}

func (n *Node) fail(err error) map[string]models.NodeResult {
	return map[string]models.NodeResult{
		errorPort: {NodeID: n.id, Data: map[string]any{"error": err.Error()}, Status: string(models.NodeStatusError), Error: err.Error()},
	}
}

func render(value any, execution *models.ExecutionContext) (any, error) {
	switch typed := value.(type) {
	case string:
		return template.RenderWithContext(typed, execution)
	case map[string]any:
		rendered := make(map[string]any, len(typed))

		for key, item := range typed {
			out, err := render(item, execution)
			if err != nil {
				return nil, err
			}

			rendered[key] = out
		}

		return rendered, nil
	case []any:
		rendered := make([]any, 0, len(typed))

		for _, item := range typed {
			out, err := render(item, execution)
			if err != nil {
				return nil, err
			}

			rendered = append(rendered, out)
		}

		return rendered, nil
	default:
		return value, nil
	}
}
