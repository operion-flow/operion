package postgresql

func migrations() map[int]string {
	return map[int]string{
		1: `
			-- Migration 1: Core workflow system
			CREATE TABLE workflows (
				id UUID PRIMARY KEY,
				name VARCHAR(255) NOT NULL,
				description TEXT NOT NULL,
				variables JSONB,
				status VARCHAR(50) NOT NULL,
				metadata JSONB,
				owner VARCHAR(255),
				workflow_group_id UUID NOT NULL,
				published_at TIMESTAMP WITH TIME ZONE,
				created_at TIMESTAMP WITH TIME ZONE NOT NULL,
				updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
				deleted_at TIMESTAMP WITH TIME ZONE
			);

			CREATE INDEX idx_workflows_status ON workflows(status);
			CREATE INDEX idx_workflows_owner ON workflows(owner);
			CREATE INDEX idx_workflows_created_at ON workflows(created_at);
			CREATE INDEX idx_workflows_deleted_at ON workflows(deleted_at);
			CREATE INDEX idx_workflows_workflow_group_id ON workflows(workflow_group_id);
			CREATE INDEX idx_workflows_published_at ON workflows(published_at);

			-- Create workflow_nodes table
			CREATE TABLE workflow_nodes (
				workflow_id UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
				id VARCHAR(255) NOT NULL,
				type VARCHAR(255) NOT NULL,
				category VARCHAR(50) NOT NULL DEFAULT 'action',
				config JSONB DEFAULT '{}',
				position_x INT DEFAULT 0,
				position_y INT DEFAULT 0,
				name VARCHAR(255) NOT NULL,
				enabled BOOLEAN NOT NULL DEFAULT true,
				source_id VARCHAR(255),
				provider_id VARCHAR(255),
				event_type VARCHAR(255),
				created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
				updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
				PRIMARY KEY (workflow_id, id)
			);

			CREATE INDEX idx_workflow_nodes_workflow_id ON workflow_nodes(workflow_id);
			CREATE INDEX idx_workflow_nodes_type ON workflow_nodes(type);
			CREATE INDEX idx_workflow_nodes_category ON workflow_nodes(category);
			CREATE INDEX idx_workflow_nodes_source_id ON workflow_nodes(source_id);
			CREATE INDEX idx_workflow_nodes_provider_id ON workflow_nodes(provider_id);

			-- Create workflow_connections table
			CREATE TABLE workflow_connections (
				workflow_id UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
				id VARCHAR(255) NOT NULL,
				source_node_id VARCHAR(255) NOT NULL,
				source_port VARCHAR(255) NOT NULL,
				target_node_id VARCHAR(255) NOT NULL,
				target_port VARCHAR(255) NOT NULL,
				created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
				updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
				PRIMARY KEY (workflow_id, id)
			);

			CREATE INDEX idx_workflow_connections_workflow_id ON workflow_connections(workflow_id);
			CREATE INDEX idx_workflow_connections_source ON workflow_connections(source_node_id);
			CREATE INDEX idx_workflow_connections_target ON workflow_connections(target_node_id);
			CREATE UNIQUE INDEX idx_workflow_connections_unique ON workflow_connections(workflow_id, source_node_id, source_port, target_node_id, target_port);

			-- Create execution_contexts table
			CREATE TABLE execution_contexts (
				id VARCHAR(255) PRIMARY KEY,
				workflow_id UUID NOT NULL REFERENCES workflows(id),
				status VARCHAR(50) NOT NULL,
				node_results JSONB DEFAULT '{}',
				trigger_data JSONB DEFAULT '{}',
				variables JSONB DEFAULT '{}',
				metadata JSONB DEFAULT '{}',
				error_message TEXT,
				created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
				completed_at TIMESTAMP WITH TIME ZONE
			);

			CREATE INDEX idx_execution_contexts_workflow_id ON execution_contexts(workflow_id);
			CREATE INDEX idx_execution_contexts_status ON execution_contexts(status);
			CREATE INDEX idx_execution_contexts_created_at ON execution_contexts(created_at);

			-- Create input_coordination_states table
			CREATE TABLE input_coordination_states (
				node_execution_id VARCHAR(255) PRIMARY KEY,
				node_id VARCHAR(255) NOT NULL,
				execution_id VARCHAR(255) NOT NULL REFERENCES execution_contexts(id) ON DELETE CASCADE,
				received_inputs JSONB DEFAULT '{}',
				requirements JSONB NOT NULL,
				created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
				last_updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
			);

			CREATE INDEX idx_input_coordination_node_execution ON input_coordination_states(node_id, execution_id);
			CREATE INDEX idx_input_coordination_execution ON input_coordination_states(execution_id);
			CREATE INDEX idx_input_coordination_created_at ON input_coordination_states(created_at);
		`,
		2: `
			-- Migration 2: Add composite indexes for ListWorkflows optimization
			-- These indexes optimize common filtering and sorting patterns in ListWorkflows

			-- Composite index for owner filtering with sorting by created_at (most common pattern)
			CREATE INDEX idx_workflows_owner_created_at ON workflows(owner, created_at DESC) WHERE deleted_at IS NULL;

			-- Composite index for status filtering with sorting by created_at
			CREATE INDEX idx_workflows_status_created_at ON workflows(status, created_at DESC) WHERE deleted_at IS NULL;

			-- Composite index for owner + status filtering with sorting by created_at
			CREATE INDEX idx_workflows_owner_status_created_at ON workflows(owner, status, created_at DESC) WHERE deleted_at IS NULL;

			-- Composite index for sorting by updated_at (second most common sort)
			CREATE INDEX idx_workflows_updated_at_desc ON workflows(updated_at DESC) WHERE deleted_at IS NULL;

			-- Composite index for owner filtering with sorting by updated_at
			CREATE INDEX idx_workflows_owner_updated_at ON workflows(owner, updated_at DESC) WHERE deleted_at IS NULL;

			-- Composite index for sorting by name (alphabetical sorting)
			CREATE INDEX idx_workflows_name_asc ON workflows(name ASC) WHERE deleted_at IS NULL;
			CREATE INDEX idx_workflows_name_desc ON workflows(name DESC) WHERE deleted_at IS NULL;

			-- Composite index for owner filtering with sorting by name
			CREATE INDEX idx_workflows_owner_name_asc ON workflows(owner, name ASC) WHERE deleted_at IS NULL;
			CREATE INDEX idx_workflows_owner_name_desc ON workflows(owner, name DESC) WHERE deleted_at IS NULL;

			-- General pagination index for unfiltered queries (covers all workflows with basic sorting)
			CREATE INDEX idx_workflows_pagination ON workflows(created_at DESC, id) WHERE deleted_at IS NULL;
		`,
	}
}
