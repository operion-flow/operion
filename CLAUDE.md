# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Operion** is a cloud-native workflow automation platform built in Go that enables event-driven workflows with node-based execution. Designed following cloud-native principles, Operion is stateless, container-first, and optimized for Kubernetes deployments. The system provides multiple interfaces: a REST API server, CLI tools, and a React-based visual editor.

## Architecture

The project follows a clean, layered architecture with clear separation of concerns:

- **Models Layer** (`pkg/models/`) - Core domain models and interfaces
- **Business Logic** (`pkg/workflow/`) - Workflow execution and management
- **Infrastructure Layer** (`pkg/persistence/`, `pkg/event_bus/`) - External integrations and data access
- **Extensions** (`pkg/registry/`) - Plugin system for nodes and providers
- **Interface Layer** (`cmd/`) - Entry points (API server, CLI tool)

### Key Domain Models

- **Workflow** - Contains nodes, connections, variables, and metadata
- **WorkflowNode** - Individual workflow nodes (triggers, actions, conditionals, etc.)
- **Node Interface** - Contract for executable nodes (unified architecture)
- **Connection** - Links between node ports for data flow
- **ExecutionContext** - Carries state between workflow nodes

### Plugin Architecture

Uses plugin-based system for extensibility:
- **Registry** - Plugin registry for nodes and providers in `pkg/registry/`
- Dynamic loading of `.so` plugin files from filesystem
- Factory pattern with `NodeFactory` and `ProviderFactory` interfaces
- Protocol-based interfaces in `pkg/protocol/` for nodes and providers
- Runtime configuration from `map[string]any`
- **Schema Support** - All NodeFactory and ProviderFactory implementations include Schema() method returning JSON Schema for configuration validation
- **Templating Examples** - All node schemas include comprehensive examples showing how to use templating with step results, trigger data, and built-in functions

## Development Commands

### Build Commands
```bash
make build          # Build API server for current platform
make build-linux    # Cross-compile for Linux
make clean          # Clean build artifacts
```

### Development Server
```bash
air                 # Start development server with live reload (proxy on port 3001, app on port 3000)
./bin/api           # Run built API server directly
```

### Environment Variables

#### API Server (operion-api)
```bash
PORT=9091              # API server port (default: 9091)
DATABASE_URL           # Database connection URL (required)
KAFKA_BROKERS          # Kafka broker addresses (required)
PLUGINS_PATH=./plugins # Path to node plugins directory (default: ./plugins)
LOG_LEVEL=info         # Log level: debug, info, warn, error (default: info)
```

**Database URL Examples:**
- File: `file:///path/to/data/directory`
- PostgreSQL: `postgres://user:password@localhost:5432/operion?sslmode=disable`

#### Worker Service (operion-worker)
```bash
WORKER_ID              # Custom worker ID (auto-generated if not provided)
DATABASE_URL           # Database connection URL (required)
KAFKA_BROKERS          # Kafka broker addresses (required)
PLUGINS_PATH=./plugins # Path to node plugins directory (default: ./plugins)
LOG_LEVEL=info         # Log level: debug, info, warn, error (default: info)
```


#### Source Manager Service (operion-source-manager)
```bash
SOURCE_MANAGER_ID         # Custom source manager ID (auto-generated if not provided)
DATABASE_URL              # Database connection URL (required)
KAFKA_BROKERS             # Kafka broker addresses (required)
PLUGINS_PATH=./plugins    # Path to source provider plugins directory (default: ./plugins)
SOURCE_PROVIDERS          # Comma-separated list of providers to run (e.g., 'scheduler,webhook')
SCHEDULER_PERSISTENCE_URL # Scheduler persistence URL (required if using scheduler): file://./data/scheduler, postgres://..., mysql://...
LOG_LEVEL=info            # Log level: debug, info, warn, error (default: info)
```

#### Activator Service (operion-activator)
```bash
ACTIVATOR_ID           # Custom activator ID (auto-generated if not provided)
DATABASE_URL           # Database connection URL (required)
KAFKA_BROKERS          # Kafka broker addresses (required)
LOG_LEVEL=info         # Log level: debug, info, warn, error (default: info)
```

### Visual Editor Development
```bash
cd ui/operion-editor    # Navigate to UI directory
npm install             # Install dependencies (first time)
npm run dev             # Start development server on port 5173
npm run build           # Build for production
npm run lint            # Run ESLint
```

### Testing and Quality
```bash
make test           # Run all tests
make test-coverage  # Generate coverage report (coverage.out and coverage.html)
make fmt            # Format Go code
make lint           # Run golangci-lint
```

### Dependencies
```bash
go mod download     # Download dependencies
go mod tidy         # Clean up dependencies
```

### Coding Standards

#### Struct Tag Alignment
All struct tags within a struct must be vertically aligned for better readability:

```go
// ✅ Correct - tags are aligned
type Example struct {
    ID          string `json:"id"          validate:"required"`
    Name        string `json:"name"        validate:"required,min=3"`
    Description string `json:"description"`
}

// ❌ Incorrect - tags are not aligned  
type Example struct {
    ID string `json:"id" validate:"required"`
    Name string `json:"name" validate:"required,min=3"`
    Description string `json:"description"`
}
```

The `tagalign` linter is configured to enforce this rule automatically. Run `make fmt` after making struct changes to ensure proper formatting.

#### Error Handling Pattern
The project uses a structured error handling pattern with typed errors for better maintainability and API consistency:

**Service Layer Errors** (`pkg/services/errors.go`):
```go
// Business logic validation errors
var (
    ErrInvalidRequest       = errors.New("invalid request")
    ErrInvalidSortField     = errors.New("invalid sort field")
    ErrWorkflowNameRequired = errors.New("workflow name is required")
    ErrNodesRequired        = errors.New("workflow must have at least one node")
)

// Structured error wrapper with context
type ServiceError struct {
    Op      string // Operation name
    Code    string // Error code for API responses
    Message string // Human-readable message
    Err     error  // Underlying error
}
```

**Error Classification**:
- Use `IsValidationError(err)` to check for business logic validation errors (400 Bad Request)
- Use `persistence.IsWorkflowNotFound(err)` for resource not found errors (404 Not Found)
- Service errors automatically map to appropriate HTTP status codes

**Handler Error Handling** (`pkg/web/errors.go`):
```go
// ✅ Correct - use typed error checking
if err != nil {
    return handleServiceError(c, err)
}

// ❌ Incorrect - avoid string-based error detection
if strings.Contains(err.Error(), "validation failed") {
    return badRequest(c, err.Error())
}
```

**Benefits**:
- Type-safe error handling with compile-time checking
- Consistent API error responses using `problems` library format
- Easy to extend with new error types without breaking existing handlers
- Robust error classification that won't break with message changes

### CI/CD Pipeline

The project uses GitHub Actions for continuous integration and quality assurance:

#### Test and Coverage Workflow (`.github/workflows/test-and-coverage.yml`)
- **Triggers**: Every pull request and push to main branch
- **Go Version**: 1.24
- **Quality Checks**:
  - Format verification with `gofmt`
  - Vet analysis with `go vet`
  - Static analysis with `staticcheck`
  - Linting with `golangci-lint`
- **Testing**:
  - Race condition detection with `-race` flag
  - Coverage generation with `-coverprofile=coverage.out`
  - Atomic coverage mode for accuracy
- **Coverage Reporting**:
  - Uploads to Codecov with project token
  - Uploads to Coveralls with GitHub token
  - HTML reports generated for artifacts
- **Build Verification**:
  - Builds all service binaries (operion-api, operion-worker, operion-activator, operion-source-manager)
  - Uploads build artifacts for download
- **Caching**: Go modules cached for faster builds

## Current Implementation Status

### Available Components
- **API Server** (`cmd/api/`) - Fiber-based REST API with workflows and registry endpoints
  - `/workflows` - CRUD operations for workflows
  - `/registry/nodes` - Sorted list of available nodes with complete JSON schemas
- **CLI Worker** (`cmd/operion-worker/`) - Background workflow execution tool
- **CLI Source Manager** (`cmd/operion-source-manager/`) - Centralized scheduler orchestrator for managing source providers
- **CLI Activator** (`cmd/operion-activator/`) - Bridge between source events and workflow events
- **Visual Workflow Editor** (`ui/operion-editor/`) - React-based browser interface for workflow visualization
- **Domain Models** (`pkg/models/`) - Core workflow and node models
- **Workflow Engine** (`pkg/workflow/`) - Workflow execution, management, and repository
- **Event System** (`pkg/event_bus/`, `pkg/events/`) - Kafka-based event-driven communication with dual topics
- **Plugin Registry** (`pkg/registry/`) - Plugin-based system for nodes and providers with .so file loading
- **File Persistence** (`pkg/persistence/file/`) - JSON file storage
- **PostgreSQL Persistence** (`pkg/persistence/postgresql/`) - PostgreSQL database storage with automated migrations

### Available Nodes
- **Trigger Nodes** (`pkg/nodes/trigger/`) - Event-based workflow initiation
  - **Scheduler** - Cron-based scheduling with robfig/cron with complete JSON schema
  - **Webhook** - HTTP webhook endpoints with centralized server management and complete JSON schema
  - **Kafka** - Kafka topic message consumption with consumer group support and complete JSON schema
- **Action Nodes** (`pkg/nodes/`) - Processing and output nodes
  - **HTTP Request** (`httprequest/`) - Make HTTP calls with retry logic and templating support
    - Schema includes: url (required), method, headers, body, retries (object with attempts/delay)
    - Templating examples: `{{.step_results.get_user_id.user_id}}`, `{{.trigger_data.webhook.url}}/callback`
    - Retry config: `{"attempts": 3, "delay": 1000}` (attempts: 0-5, delay: 100-30000ms)
  - **Transform** (`transform/`) - Process data using Go templates
    - Schema includes: expression (required), id
    - Go template examples: `{{.name}}`, `{ "fullName": "{{.firstName}} {{.lastName}}" }`, `{{len .items}}`
  - **Log** (`log/`) - Output log messages for debugging and monitoring
    - Schema includes: message (required), level
    - Templating examples: `Processing user: {{.trigger_data.webhook.user_name}}`, `{{.step_results.api_call.status}}`
  - **Conditional** (`conditional/`) - Conditional branching based on data evaluation
  - **Switch** (`switch/`) - Multi-path routing based on expression evaluation
  - **Merge** (`merge/`) - Combine multiple input streams into single output

### Database Persistence

#### PostgreSQL Implementation
- **Normalized Schema** - Separate tables for workflows, workflow_nodes, and workflow_connections
- **Automatic Migrations** - Database schema is automatically created and updated on startup via `MigrationManager`
- **Schema Versioning** - Uses `schema_migrations` table to track applied migrations
- **Soft Deletes** - Workflows are soft deleted using `deleted_at` timestamp
- **JSONB Storage** - Complex configuration data stored as JSONB, structured data in normalized tables
- **Transaction Safety** - All migrations and workflow operations run within database transactions
- **Connection Testing** - Health check endpoint verifies database connectivity
- **Modular Architecture** - Separated into `migration.go`, `workflow.go`, and main `postgres.go` files

#### Schema Structure
- **workflows** table stores core workflow data (id, name, description, variables, metadata, status, timestamps)
- **workflow_nodes** table stores node definitions with foreign key to workflows
- **workflow_connections** table stores connection definitions with foreign key to workflows
- **execution_contexts** table stores workflow execution state and results
- **input_coordination_states** table manages node input coordination for complex workflows
- **schema_migrations** table tracks migration versions and timestamps
- **UUID v7 Support** - All table IDs use time-ordered UUID v7 with auto-generation for better performance and natural sorting
- Comprehensive indexes on foreign keys, status, owner, creation time, and deletion timestamp for performance
- Cascade deletes ensure referential integrity when workflows are deleted

#### Migration System
- **MigrationManager** class handles all database schema operations
- Version-based migrations with automatic rollback on failure
- New migration versions can be added to `getMigrations()` map in `migration.go`
- Each migration runs in a transaction with proper error handling
- Migration history is preserved for audit and debugging

#### Repository Pattern
- **WorkflowRepository** handles all workflow CRUD operations
- **NodeRepository** manages individual node operations within workflows
- **ConnectionRepository** handles node connection management
- **ExecutionContextRepository** manages workflow execution state
- **InputCoordinationRepository** coordinates complex node input requirements
- Supports complex operations with nodes and connections in single transactions
- Automatic loading of related nodes and connections when retrieving workflows
- Efficient bulk operations for saving/updating workflow components

## Development Memories

- **TODO Tracking**: Check the TODO.md file to see if the implementation was described
- Always run `golangci-lint run --fix <folder or file>`, passing the folder or files that have been edited.
- Always run `go fmt ./...` after finishing editing files and before building