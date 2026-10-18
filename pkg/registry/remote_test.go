package registry

import (
	"context"
	"errors"
	"testing"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/persistence"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const benefitTypeID = "create_benefit"

type fakeRemoteTypes struct {
	types map[string]*models.RemoteNodeType
	err   error
}

func (f fakeRemoteTypes) Get(_ context.Context, id string) (*models.RemoteNodeType, error) {
	if f.err != nil {
		return nil, f.err
	}

	nodeType, ok := f.types[id]
	if !ok {
		return nil, persistence.ErrRemoteNodeTypeNotFound
	}

	return nodeType, nil
}

func TestCreateNodeFallsBackToRemoteTypes(t *testing.T) {
	reg := NewRegistry()
	reg.UseRemoteTypes(fakeRemoteTypes{types: map[string]*models.RemoteNodeType{
		benefitTypeID: {ID: benefitTypeID, URL: "http://benefits.test", Outputs: []string{"approved"}},
	}})

	node, err := reg.CreateNode(context.Background(), benefitTypeID, "benefit", nil)

	require.NoError(t, err)
	assert.Equal(t, benefitTypeID, node.Type())
	assert.Len(t, node.OutputPorts(), 2)
}

func TestCreateNodeWithoutRemoteType(t *testing.T) {
	failure := errors.New("database is down")

	tests := []struct {
		name   string
		remote RemoteTypes
		want   error
	}{
		{name: "no resolver", remote: nil, want: ErrNodeNotRegistered},
		{name: "unknown type", remote: fakeRemoteTypes{}, want: ErrNodeNotRegistered},
		{name: "resolver error", remote: fakeRemoteTypes{err: failure}, want: failure},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := NewRegistry()
			if tt.remote != nil {
				reg.UseRemoteTypes(tt.remote)
			}

			_, err := reg.CreateNode(context.Background(), benefitTypeID, "benefit", nil)

			require.ErrorIs(t, err, tt.want)
		})
	}
}
