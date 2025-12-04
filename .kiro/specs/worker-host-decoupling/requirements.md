# Requirements Document

## Introduction

This document specifies the requirements for refactoring the `deadline-cloud-test-fixtures` Python library to decouple the EC2 host machine from the worker agent configuration. Currently, the library tightly couples EC2 instance provisioning with worker agent installation and configuration. This refactoring will enable test developers to reuse EC2 host machines with different worker agent configurations, improving test efficiency and reducing resource costs.

**Scope**: This refactoring applies only to EC2-based workers. Docker-based workers (`DockerContainerWorker`) are excluded from this refactor as containers are inexpensive to launch and do not benefit from host reuse.

## Glossary

- **Worker Host**: An EC2 instance that provides the computing environment for a worker agent
- **Worker Agent**: The Deadline Cloud software that runs on a worker host to communicate with AWS Deadline Cloud service and execute work
- **Worker**: The logical combination of a worker host and a configured worker agent registered with Deadline Cloud
- **Worker Configuration**: The settings and parameters used to configure a worker agent (farm ID, fleet ID, region, users, etc.)
- **Test Fixture**: A pytest fixture that provides test resources
- **Worker Host Lifecycle**: The operations of starting, stopping, and managing an EC2 worker host machine
- **Worker Agent Lifecycle**: The operations of installing, configuring, starting, and stopping the worker agent software
- **DeadlineWorker**: The current class that manages both worker host and worker agent together; will be refactored to delegate host operations to WorkerHost
- **EC2InstanceWorker**: The base class for EC2-based workers that will be refactored to use WorkerHost composition

## Requirements

### Requirement 1

**User Story:** As a test developer, I want to provision a host machine once and reuse it with different worker agent configurations, so that I can reduce test execution time and infrastructure costs.

#### Acceptance Criteria

1. WHEN a test developer provisions a worker host THEN the system SHALL create the worker host without automatically installing or configuring a worker agent
2. WHEN a worker host is provisioned THEN the system SHALL allow multiple worker agent configurations to be applied to that worker host sequentially
3. WHEN a worker agent configuration is applied to a worker host THEN the system SHALL install and configure the worker agent according to the provided configuration
4. WHEN a worker agent is stopped on a worker host THEN the system SHALL allow a new worker agent configuration to be applied to the same worker host
5. WHEN a worker host is stopped THEN the system SHALL clean up the worker host resources without requiring worker agent cleanup

### Requirement 2

**User Story:** As a test developer, I want separate control over host lifecycle and worker agent lifecycle, so that I can manage them independently based on my testing needs.

#### Acceptance Criteria

1. WHEN managing a worker host THEN the system SHALL provide operations to start, stop, and send commands to the worker host
2. WHEN managing a worker agent THEN the system SHALL provide operations to install, configure, start, stop, and retrieve the worker ID
3. WHEN a worker host is started THEN the system SHALL NOT automatically start a worker agent
4. WHEN a worker agent is stopped THEN the system SHALL NOT automatically stop the worker host
5. WHEN a worker host is reused THEN the system SHALL allow the previous worker agent to be stopped and a new one to be configured

### Requirement 3

**User Story:** As a test developer, I want the refactored architecture to maintain backward compatibility with existing tests, so that I don't have to rewrite all my existing test code.

#### Acceptance Criteria

1. WHEN existing tests use the current worker fixture THEN the system SHALL continue to function without modification
2. WHEN the worker fixture is used THEN the system SHALL provide the same interface as the current DeadlineWorker class
3. WHEN tests access worker methods THEN the system SHALL delegate to the appropriate host or worker agent manager
4. WHEN tests use EC2InstanceWorker classes directly THEN the system SHALL maintain the existing behavior
5. WHEN the system creates a worker through the fixture THEN the system SHALL automatically manage both host and worker agent lifecycles as before

### Requirement 4

**User Story:** As a test developer, I want to create host-only fixtures that don't include worker agent configuration, so that I can test different worker agent scenarios on the same host.

#### Acceptance Criteria

1. WHEN a test requests a worker host fixture THEN the system SHALL provide a worker host without a worker agent
2. WHEN a worker host is provided THEN the system SHALL expose methods to start, stop, and send commands to the worker host
3. WHEN a test applies a worker agent configuration to a worker host THEN the system SHALL install and configure the worker agent on that worker host
4. WHEN a test stops a worker agent THEN the system SHALL leave the worker host running and available for reuse
5. WHEN a test completes THEN the system SHALL clean up the worker host if it was created by the fixture

### Requirement 5

**User Story:** As a library maintainer, I want the new architecture to separate concerns between host management and worker agent management, so that the codebase is easier to maintain and extend.

#### Acceptance Criteria

1. WHEN implementing worker host functionality THEN the system SHALL define a WorkerHost interface with methods for worker host lifecycle operations
2. WHEN implementing DeadlineWorker classes THEN the system SHALL delegate worker host operations to a WorkerHost instance
3. WHEN a WorkerHost implementation is created THEN the system SHALL NOT contain worker agent configuration logic
4. WHEN a DeadlineWorker implementation manages worker agent lifecycle THEN the system SHALL NOT contain worker host provisioning logic
5. WHEN the system combines a worker host and worker agent THEN the system SHALL use composition with DeadlineWorker containing a WorkerHost instance

### Requirement 6

**User Story:** As a test developer, I want to configure worker agents with different settings on the same host, so that I can test worker behavior under various configurations without reprovisioning infrastructure.

#### Acceptance Criteria

1. WHEN a worker agent is configured on a worker host THEN the system SHALL accept a DeadlineWorkerConfiguration object
2. WHEN a worker agent is stopped THEN the system SHALL clean up the worker agent state on the worker host
3. WHEN a new worker agent configuration is applied THEN the system SHALL install and configure the worker agent with the new settings
4. WHEN multiple worker agents are configured sequentially THEN the system SHALL ensure each configuration is independent
5. WHEN a worker agent is reconfigured THEN the system SHALL update the worker registration with Deadline Cloud service

### Requirement 7

**User Story:** As a test developer, I want clear error messages when host or worker agent operations fail, so that I can quickly diagnose and fix issues.

#### Acceptance Criteria

1. WHEN a worker host operation fails THEN the system SHALL raise an exception with diagnostic information about the worker host state
2. WHEN a worker agent operation fails THEN the system SHALL raise an exception with diagnostic information about the worker agent state
3. WHEN a command sent to a worker host fails THEN the system SHALL include the command output in the error message
4. WHEN a worker agent fails to start THEN the system SHALL include relevant log information in the error message
5. WHEN an operation fails THEN the system SHALL distinguish between worker-host-related and worker-agent-related failures

### Requirement 8

**User Story:** As a library maintainer, I want the refactored code to support both Windows and POSIX EC2 host types, so that tests can run on different operating systems.

#### Acceptance Criteria

1. WHEN implementing EC2 worker host functionality THEN the system SHALL support all current EC2 worker functionality
2. WHEN implementing Windows EC2 worker host functionality THEN the system SHALL support all current Windows EC2 worker functionality
3. WHEN implementing POSIX EC2 worker host functionality THEN the system SHALL support all current POSIX EC2 worker functionality
4. WHEN a worker host platform is selected THEN the system SHALL provide the appropriate WorkerHost implementation
5. WHEN extending to new worker host types THEN the system SHALL allow new WorkerHost implementations without modifying worker agent code

### Requirement 9

**User Story:** As a consumer of the library who has extended WindowsInstanceWorkerBase or PosixInstanceWorkerBase, I want these public API classes to maintain their interface and behavior, so that my existing code continues to work without modification.

#### Acceptance Criteria

1. WHEN the refactoring is complete THEN the system SHALL preserve the WindowsInstanceWorkerBase class as a public API
2. WHEN the refactoring is complete THEN the system SHALL preserve the PosixInstanceWorkerBase class as a public API
3. WHEN external code inherits from WindowsInstanceWorkerBase THEN the system SHALL maintain all existing abstract methods and their signatures
4. WHEN external code inherits from PosixInstanceWorkerBase THEN the system SHALL maintain all existing abstract methods and their signatures
5. WHEN external code uses instances of WindowsInstanceWorkerBase or PosixInstanceWorkerBase THEN the system SHALL maintain all existing public methods and their behavior
