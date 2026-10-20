package ingest

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/bufbuild/protocompile"
	"github.com/dukex/operion/pkg/models"
	"github.com/hamba/avro/v2"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

const (
	confluentHeader     = 5
	protobufSchemaFile  = "schema.proto"
	schemaRegistryLimit = 10 * time.Second
)

var (
	errNoSchemaRegistry    = errors.New("avro and protobuf events need a schema registry")
	errNotConfluent        = errors.New("value is not in the Confluent wire format")
	errSchemaNotFound      = errors.New("schema not found in the schema registry")
	errRegistryUnavailable = errors.New("schema registry is unavailable")
	errMessageIndexes      = errors.New("protobuf message indexes are malformed")
	errMessageNotFound     = errors.New("protobuf message not found in the schema")
)

type SchemaRegistry struct {
	URL      string
	Username string
	Password string
}

type schemaKey struct {
	format models.EventFormat
	id     uint32
}

type decodeFunc func(payload []byte) ([]byte, error)

type decoder struct {
	registry SchemaRegistry
	client   *http.Client
	schemas  sync.Map
}

func newDecoder(registry SchemaRegistry) *decoder {
	return &decoder{registry: registry, client: &http.Client{Timeout: schemaRegistryLimit}}
}

func (d *decoder) decode(ctx context.Context, format models.EventFormat, value []byte) ([]byte, error) {
	if format == models.EventFormatJSON {
		return value, nil
	}

	if d.registry.URL == "" {
		return nil, fmt.Errorf("%w: %s", errNoSchemaRegistry, format)
	}

	if len(value) < confluentHeader || value[0] != 0 {
		return nil, errNotConfluent
	}

	key := schemaKey{format: format, id: binary.BigEndian.Uint32(value[1:confluentHeader])}

	decode, ok := d.schemas.Load(key)
	if !ok {
		compiled, err := d.compile(ctx, key)
		if err != nil {
			return nil, err
		}

		decode, _ = d.schemas.LoadOrStore(key, compiled)
	}

	return decode.(decodeFunc)(value[confluentHeader:])
}

func (d *decoder) compile(ctx context.Context, key schemaKey) (decodeFunc, error) {
	schema, err := d.fetch(ctx, key.id)
	if err != nil {
		return nil, err
	}

	if key.format == models.EventFormatAvro {
		return avroDecoder(schema)
	}

	return protobufDecoder(ctx, schema)
}

func (d *decoder) fetch(ctx context.Context, id uint32) (string, error) {
	address := fmt.Sprintf("%s/schemas/ids/%d", strings.TrimSuffix(d.registry.URL, "/"), id)

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
	if err != nil {
		return "", fmt.Errorf("%w: %w", errRegistryUnavailable, err)
	}

	if d.registry.Username != "" {
		request.SetBasicAuth(d.registry.Username, d.registry.Password)
	}

	response, err := d.client.Do(request)
	if err != nil {
		return "", fmt.Errorf("%w: %w", errRegistryUnavailable, err)
	}

	defer func() { _ = response.Body.Close() }()

	switch response.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return "", fmt.Errorf("%w: %d", errSchemaNotFound, id)
	default:
		return "", fmt.Errorf("%w: %s", errRegistryUnavailable, response.Status)
	}

	var body struct {
		Schema string `json:"schema"`
	}

	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		return "", fmt.Errorf("%w: %w", errRegistryUnavailable, err)
	}

	return body.Schema, nil
}

func avroDecoder(text string) (decodeFunc, error) {
	schema, err := avro.ParseWithCache(text, "", &avro.SchemaCache{})
	if err != nil {
		return nil, err
	}

	return func(payload []byte) ([]byte, error) {
		var value any
		if err := avro.Unmarshal(schema, payload, &value); err != nil {
			return nil, err
		}

		return json.Marshal(value)
	}, nil
}

func protobufDecoder(ctx context.Context, text string) (decodeFunc, error) {
	compiler := protocompile.Compiler{Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
		Accessor: protocompile.SourceAccessorFromMap(map[string]string{protobufSchemaFile: text}),
	})}

	files, err := compiler.Compile(ctx, protobufSchemaFile)
	if err != nil {
		return nil, err
	}

	return func(payload []byte) ([]byte, error) {
		indexes, payload, err := messageIndexes(payload)
		if err != nil {
			return nil, err
		}

		message, err := protobufMessage(files[0].Messages(), indexes)
		if err != nil {
			return nil, err
		}

		if err := proto.Unmarshal(payload, message); err != nil {
			return nil, err
		}

		return protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}.Marshal(message)
	}, nil
}

func messageIndexes(payload []byte) ([]int64, []byte, error) {
	count, payload, err := varint(payload)
	if count <= 0 {
		return []int64{0}, payload, err
	}

	indexes := []int64{}

	for range count {
		var index int64
		if index, payload, err = varint(payload); err != nil {
			return nil, nil, err
		}

		indexes = append(indexes, index)
	}

	return indexes, payload, nil
}

func varint(payload []byte) (int64, []byte, error) {
	value, read := binary.Varint(payload)
	if read <= 0 {
		return 0, nil, errMessageIndexes
	}

	return value, payload[read:], nil
}

func protobufMessage(messages protoreflect.MessageDescriptors, indexes []int64) (*dynamicpb.Message, error) {
	var descriptor protoreflect.MessageDescriptor

	for _, index := range indexes {
		if index < 0 || index >= int64(messages.Len()) {
			return nil, fmt.Errorf("%w: %v", errMessageNotFound, indexes)
		}

		descriptor = messages.Get(int(index))
		messages = descriptor.Messages()
	}

	return dynamicpb.NewMessage(descriptor), nil
}
