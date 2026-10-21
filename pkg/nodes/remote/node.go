package remote

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"time"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/protocol"
	"github.com/dukex/operion/pkg/template"
)

const (
	inputPort       = "main"
	successPort     = "success"
	errorPort       = "error"
	defaultTimeout  = 30
	maxResponseSize = 1 << 20
	timestampHeader = "X-Operion-Timestamp"
	signatureHeader = "X-Operion-Signature"
	defaultAnswer   = 24 * time.Hour
	maxAnswer       = 365 * 24 * time.Hour
	tokenKey        = "callback_token_sha256"
)

var (
	errNoCallbackURL = errors.New("remote node answered 202 but serve has no --public-url for it to call back")
	errNoAnswer      = errors.New("remote node did not call back before its deadline")
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
	nodeType     models.RemoteNodeType
	id           string
	config       map[string]any
	client       *http.Client
	callbackBase string
}

func New(nodeType models.RemoteNodeType, id string, config map[string]any, client *http.Client, callbackBase string) *Node {
	Normalize(&nodeType)

	return &Node{nodeType: nodeType, id: id, config: config, client: client, callbackBase: callbackBase}
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
	CallbackURL string         `json:"callback_url,omitempty"`
}

type response struct {
	Port           string         `json:"port"`
	Data           map[string]any `json:"data"`
	TimeoutSeconds int            `json:"timeout_seconds"`
}

func (n *Node) Execute(execution models.ExecutionContext, inputs map[string]models.NodeResult) (map[string]models.NodeResult, error) {
	if answer, ok := inputs[protocol.ResumePort]; ok {
		return n.answered(answer.Data), nil
	}

	config, err := render(n.config, &execution)
	if err != nil {
		return n.fail(fmt.Errorf("config template: %w", err)), nil
	}

	nodeResults := make(map[string]any, len(execution.NodeResults))
	for nodeID, result := range execution.NodeResults {
		nodeResults[nodeID] = result.Data
	}

	token := rand.Text()

	body, err := json.Marshal(request{
		ExecutionID: execution.ID,
		WorkflowID:  execution.WorkflowID,
		NodeID:      n.id,
		Config:      config,
		Input:       inputs[inputPort].Data,
		TriggerData: execution.TriggerData,
		NodeResults: nodeResults,
		Variables:   execution.Variables,
		CallbackURL: n.callbackURL(execution.ID, token),
	})
	if err != nil {
		return n.fail(fmt.Errorf("encode request: %w", err)), nil
	}

	answer, accepted, err := n.call(body)
	if err != nil {
		return n.fail(err), nil
	}

	if accepted {
		return n.suspend(answer, token)
	}

	return n.route(answer), nil
}

func (n *Node) callbackURL(executionID, token string) string {
	if n.callbackBase == "" {
		return ""
	}

	return n.callbackBase + "/callbacks/" + url.PathEscape(executionID) + "/" + url.PathEscape(n.id) + "/" + token
}

func (n *Node) suspend(answer *response, token string) (map[string]models.NodeResult, error) {
	if n.callbackBase == "" {
		return n.fail(errNoCallbackURL), nil
	}

	wait := defaultAnswer
	if answer.TimeoutSeconds != 0 {
		wait = time.Duration(answer.TimeoutSeconds) * time.Second
	}

	if wait <= 0 || wait > maxAnswer {
		return n.fail(fmt.Errorf("remote node asked to wait %d seconds, more than a year or not positive", answer.TimeoutSeconds)), nil
	}

	until := time.Now().Add(wait).UTC()

	return nil, &protocol.SuspendError{Until: until, Data: map[string]any{
		tokenKey:    tokenHash(token),
		"answer_by": until.Format(time.RFC3339),
	}}
}

func (n *Node) answered(data map[string]any) map[string]models.NodeResult {
	if data == nil {
		return n.fail(errNoAnswer)
	}

	port, _ := data["port"].(string)
	result, _ := data["data"].(map[string]any)

	return n.route(&response{Port: port, Data: result})
}

func (n *Node) route(answer *response) map[string]models.NodeResult {
	if !slices.Contains(n.nodeType.Outputs, answer.Port) {
		return n.fail(fmt.Errorf("remote node answered unknown port %q", answer.Port))
	}

	status := models.NodeStatusSuccess
	if answer.Port == errorPort {
		status = models.NodeStatusError
	}

	return map[string]models.NodeResult{answer.Port: {NodeID: n.id, Data: answer.Data, Status: string(status)}}
}

// AwaitsCallback reports whether result is a remote node waiting for its service to call back, which only an answer naming a port can resume.
func AwaitsCallback(result models.NodeResult) bool {
	stored, _ := result.Data[tokenKey].(string)

	return stored != ""
}

// CallbackAllowed reports whether token is the callback token handed out with the call that left result waiting.
func CallbackAllowed(result models.NodeResult, token string) bool {
	stored, _ := result.Data[tokenKey].(string)

	return stored != "" && subtle.ConstantTimeCompare([]byte(stored), []byte(tokenHash(token))) == 1
}

func tokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))

	return hex.EncodeToString(sum[:])
}

func (n *Node) call(body []byte) (*response, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(n.nodeType.TimeoutSeconds)*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.nodeType.URL, bytes.NewReader(body))
	if err != nil {
		return nil, false, err
	}

	req.Header.Set("Content-Type", "application/json")

	if n.nodeType.Secret != "" {
		timestamp := strconv.FormatInt(time.Now().Unix(), 10)
		req.Header.Set(timestampHeader, timestamp)
		req.Header.Set(signatureHeader, sign(n.nodeType.Secret, timestamp, body))
	}

	resp, err := n.client.Do(req)
	if err != nil {
		return nil, false, err
	}

	defer func() { _ = resp.Body.Close() }()

	payload, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseSize))
	if err != nil {
		return nil, false, err
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, false, fmt.Errorf("remote node answered status %d: %s", resp.StatusCode, payload)
	}

	accepted := resp.StatusCode == http.StatusAccepted

	var answer response
	if accepted && len(bytes.TrimSpace(payload)) == 0 {
		return &answer, true, nil
	}

	if err := json.Unmarshal(payload, &answer); err != nil {
		return nil, false, fmt.Errorf("remote node answered invalid JSON: %w", err)
	}

	return &answer, accepted, nil
}

func sign(secret, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp + "."))
	mac.Write(body)

	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
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
