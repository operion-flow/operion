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
			CREATE TABLE jobs (
				id UUID PRIMARY KEY,
				execution_id VARCHAR(255) NOT NULL REFERENCES execution_contexts(id) ON DELETE CASCADE,
				node_id VARCHAR(255) NOT NULL,
				port VARCHAR(255) NOT NULL,
				data JSONB NOT NULL DEFAULT '{}',
				run_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
				created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
			);

			CREATE INDEX idx_jobs_run_at ON jobs(run_at);
			CREATE INDEX idx_jobs_execution_id ON jobs(execution_id);
		`,
		3: `
			CREATE TABLE schedules (
				workflow_id UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
				node_id VARCHAR(255) NOT NULL,
				next_run_at TIMESTAMP WITH TIME ZONE NOT NULL,
				PRIMARY KEY (workflow_id, node_id)
			);

			CREATE INDEX idx_schedules_next_run_at ON schedules(next_run_at);
		`,
		4: `
			DROP TABLE IF EXISTS input_coordination_states;
		`,
		5: `
			CREATE TABLE event_definitions (
				key VARCHAR(255) PRIMARY KEY,
				name VARCHAR(255) NOT NULL,
				description TEXT NOT NULL DEFAULT '',
				topic VARCHAR(255) NOT NULL,
				format VARCHAR(50) NOT NULL,
				schema JSONB,
				tenant_from VARCHAR(50) NOT NULL,
				tenant_key VARCHAR(255) NOT NULL,
				created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
				updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
			);

			CREATE INDEX idx_event_definitions_topic ON event_definitions(topic);
		`,
		6: `
			ALTER TABLE workflows ADD COLUMN tenant_id VARCHAR(255) NOT NULL DEFAULT '';

			CREATE INDEX idx_workflows_tenant_id ON workflows(tenant_id);
		`,
		8: `
			CREATE TABLE remote_node_types (
				id VARCHAR(255) PRIMARY KEY,
				name VARCHAR(255) NOT NULL,
				description TEXT NOT NULL DEFAULT '',
				url TEXT NOT NULL,
				schema JSONB,
				outputs JSONB NOT NULL,
				timeout_seconds INTEGER NOT NULL,
				created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
				updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
			);
		`,
	}
}
