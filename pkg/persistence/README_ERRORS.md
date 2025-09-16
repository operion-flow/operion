# Standardized Error Handling for Persistence Layer

## Overview

This document describes the standardized error handling system implemented across the persistence layer to replace fragile string-based error checking with robust, type-safe error handling.

## Problem Statement

The original implementation used string-based error checking, which was:
- **Brittle**: Different persistence implementations returned different error messages
- **Inconsistent**: No standard error types across implementations  
- **Hard to Test**: Tests failed due to slight variations in error strings
- **Maintenance Heavy**: Changes to error messages broke existing code

## Solution: Standardized Error Types

### Core Error Constants

All persistence implementations now use standardized error constants:

```go
// Standard persistence error types
var (
    ErrWorkflowNotFound          = errors.New("workflow not found")
    ErrPublishedWorkflowNotFound = errors.New("published workflow not found")
    ErrDraftWorkflowNotFound     = errors.New("draft workflow not found")
    ErrNodeNotFound              = errors.New("node not found")
    ErrConnectionNotFound        = errors.New("connection not found")
    // ... more standardized errors
)
```

### Contextual Error Wrappers

Standardized error wrappers provide additional context:

```go
// WorkflowError wraps workflow-related errors with context
type WorkflowError struct {
    Op         string // Operation being performed
    WorkflowID string // Workflow ID if applicable  
    GroupID    string // Workflow group ID if applicable
    Err        error  // Underlying error
    Message    string // Additional context message
}
```

### Error Checking Functions

Type-safe error checking functions:

```go
// Check specific error types
func IsWorkflowNotFound(err error) bool
func IsPublishedWorkflowNotFound(err error) bool  
func IsDraftWorkflowNotFound(err error) bool
```

## Implementation Examples

### Repository Layer

**Before (String-based):**
```go
if publishedWorkflow == nil {
    return nil, fmt.Errorf("no published workflow found for group: %s", groupID)
}
```

**After (Standardized):**
```go
if publishedWorkflow == nil {
    return nil, persistence.NewWorkflowGroupError("CreateDraftFromPublished", groupID, 
        persistence.ErrPublishedWorkflowNotFound)
}
```

### Service Layer  

**Before (String checking):**
```go
if err != nil {
    if strings.Contains(err.Error(), "not found") {
        return notFound(c, "Workflow not found")
    }
    return internalError(c, err)
}
```

**After (Type-safe):**
```go
if err != nil {
    if persistence.IsPublishedWorkflowNotFound(err) {
        return notFound(c, "Published workflow not found")
    }
    return internalError(c, err)
}
```

## Benefits

### 1. **Consistency**
- All implementations return the same error types
- Consistent error messages across the system
- Uniform error handling patterns

### 2. **Robustness** 
- Type-safe error checking using `errors.Is()`
- No dependency on exact string matching
- Works correctly with error wrapping

### 3. **Context**
- Rich error information with operation context
- Workflow/group IDs included in error messages
- Clear error messages for debugging

### 4. **Testability**
- Predictable error types in tests
- No more flaky tests due to string variations
- Easy to mock and verify specific errors

### 5. **Maintainability**
- Changes to error messages don't break existing code
- Centralized error definitions
- Easy to add new standardized errors

## Usage Guidelines

### For Repository Implementations

1. **Always use standardized error constants:**
   ```go
   return persistence.ErrWorkflowNotFound
   ```

2. **Provide context with wrappers:**
   ```go
   return persistence.NewWorkflowError("GetByID", workflowID, persistence.ErrWorkflowNotFound)
   ```

3. **Use appropriate error types for operations:**
   - `ErrWorkflowNotFound` for missing workflows by ID
   - `ErrPublishedWorkflowNotFound` for missing published workflows
   - `ErrDraftWorkflowNotFound` for missing draft workflows

### For Service Layer

1. **Use type-safe error checking:**
   ```go
   if persistence.IsWorkflowNotFound(err) {
       // handle workflow not found
   }
   ```

2. **Don't rely on string matching:**
   ```go
   // DON'T DO THIS:
   if strings.Contains(err.Error(), "not found") { ... }
   
   // DO THIS:
   if persistence.IsWorkflowNotFound(err) { ... }
   ```

### For HTTP Handlers

1. **Map persistence errors to HTTP status codes:**
   ```go
   if persistence.IsWorkflowNotFound(err) {
       return notFound(c, "Workflow not found")
   }
   if persistence.IsPublishedWorkflowNotFound(err) {
       return notFound(c, "Published workflow not found")  
   }
   ```

## Error Mapping

| Persistence Error | HTTP Status | Description |
|-------------------|-------------|-------------|
| `ErrWorkflowNotFound` | 404 Not Found | Workflow doesn't exist |
| `ErrPublishedWorkflowNotFound` | 404 Not Found | No published version exists |
| `ErrDraftWorkflowNotFound` | 404 Not Found | No draft version exists |
| `ErrWorkflowAlreadyExists` | 409 Conflict | Workflow with same ID exists |
| `ErrInvalidWorkflowStatus` | 400 Bad Request | Invalid status transition |

## Future Extensions

The error handling system is designed to be extensible:

1. **Add new error types** by defining constants in `errors.go`
2. **Create specialized error wrappers** for different domains (nodes, connections, etc.)  
3. **Add error checking functions** following the `Is*Error(err error) bool` pattern
4. **Extend error context** by adding fields to existing error types

## Testing

The standardized error system includes comprehensive tests:

```bash
go test ./pkg/persistence/ -v  # Test error constants and functions
go test ./pkg/web/ -v         # Test HTTP error handling
```

All tests now pass consistently across different persistence implementations, proving the robustness of the standardized approach.