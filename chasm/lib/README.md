# CHASM Archetype Implementations

This directory contains implementations of various archetypes built on the CHASM framework. Each subdirectory represents a complete archetype implementation with its components, tasks, and API handlers.

## Overview

CHASM archetypes are packaged as libraries that provide:
- **Root Component**: The main component type that represents the execution (e.g., Scheduler, Callback)
- **Sub-Components**: Child components that implement specific functionality
- **Tasks**: Pure and side-effect task executors for event processing
- **API Handlers**: gRPC service implementations for external interaction
- **State Definitions**: Protobuf messages defining component state

## Directory Structure

```
lib/
├── callback/          # Callback archetype for Nexus operations
├── scheduler/         # Scheduler archetype for scheduled workflow execution
├── tests/            # Test utilities and mock components
└── workflow/         # Workflow archetype (work in progress)
```

## Available Archetypes

### Scheduler (`lib/scheduler/`)

Schedule-based workflow execution with cron scheduling, backfills, and result tracking.

**See [`scheduler/README.md`](scheduler/README.md) for detailed documentation.**

### Callback (`lib/callback/`)

Asynchronous callback handling for Nexus operations with timeout management and invocation tracking.

**See [`callback/README.md`](callback/README.md) for detailed documentation.**

### Tests (`lib/tests/`)

Test utilities and mock components for testing CHASM functionality. Referenced by integration tests.

### Workflow (`lib/workflow/`)  

Workflow execution state machine (work in progress - not yet documented).

## Archetype Library Structure

Each archetype follows a consistent structure:

```
lib/myarchetype/
├── proto/v1/              # Protobuf definitions
├── gen/myarchetypepb/v1/  # Generated code
├── library.go             # Library implementation
├── component.go           # Root and sub-components
├── tasks.go               # Task executors
├── handler.go             # gRPC API handler
├── config.go              # Configuration
├── fx.go                  # Dependency injection
└── README.md              # Detailed documentation
```

For implementation details and examples, see the individual archetype documentation:
- [`scheduler/README.md`](scheduler/README.md) - Complex multi-component archetype
- [`callback/README.md`](callback/README.md) - Simpler archetype example

---

**Next**: See individual archetype READMEs for detailed documentation.
