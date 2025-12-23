# Implementation Plan

## Implementation Principles

**Continuous Integration Compatibility**: All tasks must maintain passing tests throughout implementation. When making breaking changes to code, simultaneously update affected tests in the same task to ensure the test suite never fails.

**Test-First Approach**: Write new tests before implementing new functionality, and update existing tests alongside code changes to maintain green builds at every step.

**After Each Task Completion**: Run the full test suite to verify all tests pass before proceeding to the next task. Each task should leave the codebase in a fully functional state with no failing tests.

## Phase 1: Create WorkerHost Abstraction

- [ ] 1. Create WorkerHost abstract base class, state enums, and property test
  - Create `WorkerHost` abstract base class with lifecycle methods (`start()`, `stop()`, `send_command()`)
  - Create `WorkerHostState` enum (`NOT_STARTED`, `RUNNING`, `STOPPED`)
  - Create `WorkerAgentState` enum (`NOT_STARTED`, `RUNNING`, `STOPPED`)
  - Implement state validation and ownership tracking in WorkerHost
  - Write property test for WorkerHost state management (**Property 1: Worker host creation independence**)
  - **Simultaneously ensure all existing tests continue to pass**
  - _Requirements: 2.1, 3.2, 4.1_
  - _Validates: Requirements 1.1, 2.3_

- [ ] 2. Create EC2WorkerHost base class and property test
  - Create `EC2WorkerHost` abstract base class extending `WorkerHost`
  - Move EC2-specific fields from `EC2InstanceWorker` to `EC2WorkerHost`
  - Implement common EC2 functionality (AMI resolution, instance launching, SSM commands)
  - Add abstract methods for OS-specific behavior (`ami_ssm_param_name()`, `ssm_document_name()`, etc.)
  - Write property test for EC2WorkerHost interface (**Property 15: WorkerHost interface completeness**)
  - **Simultaneously ensure all existing tests continue to pass**
  - _Requirements: 7.1, 7.2, 7.3_
  - _Validates: Requirements 2.1, 3.2, 4.1_

- [ ] 3. Create OS-specific WorkerHost implementations and unit tests
  - Create `WindowsEC2WorkerHost` class with Windows-specific behavior
  - Create `PosixEC2WorkerHost` class with POSIX-specific behavior
  - Move OS-specific methods from existing worker classes to WorkerHost implementations
  - Implement `_operating_system()` method for each OS type
  - Write unit tests for `WindowsEC2WorkerHost` and `PosixEC2WorkerHost`
  - Test OS-specific behavior (SSM documents, userdata, EBS devices)
  - Test state transitions and error handling
  - **Simultaneously ensure all existing tests continue to pass**
  - _Requirements: 7.1, 7.2, 7.3, 7.4_

- [ ] 3.1 Implement userdata success checking in WorkerHost and add property test
  - Add `userdata_success_script()` abstract method to `EC2WorkerHost`
  - Add `_wait_until_userdata_finishes()` method to `EC2WorkerHost`
  - Add userdata success/failure constants to `EC2WorkerHost`
  - Implement OS-specific `userdata_success_script()` methods in `WindowsEC2WorkerHost` and `PosixEC2WorkerHost`
  - Update `_do_start()` to call `_wait_until_userdata_finishes()` after instance launch
  - Write property test for userdata completion validation (**Property 2: Userdata completion validation**)
  - **Simultaneously update any affected tests to maintain passing status**
  - _Requirements: 2.1, 6.1_
  - _Validates: Requirements 2.1, 6.1_

## Phase 2: Refactor EC2InstanceWorker Classes

- [ ] 4. Add WorkerHost composition to EC2InstanceWorker and property test
  - Add `worker_host: EC2WorkerHost` field to `EC2InstanceWorker`
  - Add `WorkerAgentState` tracking to `EC2InstanceWorker`
  - Implement OS validation in `__post_init__()` method
  - Add abstract `_required_host_os()` method
  - Write property test for composition architecture (**Property 19: Composition architecture**)
  - **Simultaneously update any affected tests to maintain passing status**
  - _Requirements: 4.2, 4.5_
  - _Validates: Requirements 4.5_

- [ ] 5. Refactor EC2InstanceWorker lifecycle methods, add property tests, and update existing tests
  - Refactor `start()` method to only handle worker agent lifecycle
  - Refactor `stop()` method to only handle worker agent cleanup
  - Add host ownership claiming/releasing logic
  - Delegate `send_command()` to WorkerHost
  - Write property test for agent lifecycle independence (**Property 6: Worker agent stop preserves host**)
  - Write property test for method delegation (**Property 7: Method delegation correctness**)
  - **Simultaneously update all existing tests for new architecture:**
    - Update `TestPosixInstanceBuildWorker` tests to work with new composition-based API
    - Fix test fixtures to create `PosixEC2WorkerHost` and pass to worker constructor
    - Update tests to use `worker_host.start()` before `worker.start()`
    - Update send_command tests to use `worker_host.ssm_client` instead of `worker.ssm_client`
    - Update instance_id references to use `worker_host.instance_id` instead of `worker.instance_id`
    - Update ami_id references to use `worker_host.ami_id` instead of `worker.ami_id`
    - Remove obsolete tests (`test_stage_s3_bucket`, `test_launch_instance`, `test_setup_worker_agent`)
    - Update retry behavior test to match new EC2WorkerHost implementation
  - **Ensure all tests pass throughout the refactoring process**
  - _Requirements: 1.2, 2.2, 2.4, 2.5_
  - _Validates: Requirements 2.4, 4.4, 4.2_

- [ ] 6. Refactor both WindowsInstanceWorkerBase and PosixInstanceWorkerBase for agent-only operations
  - Update `WindowsInstanceWorkerBase` to use `WindowsEC2WorkerHost`
  - Update `PosixInstanceWorkerBase` to use `PosixEC2WorkerHost`
  - Implement `_required_host_os()` to return "windows" and "posix" respectively
  - Move agent-specific methods (`_install_agent()`, `_configure_agent()`) from both base classes
  - Update constructors to require `worker_host` parameter
  - **Simultaneously update any affected tests to maintain passing status**
  - _Requirements: 4.3, 4.4, 7.2, 7.3_

- [ ] 6.1 Write property test for OS-agnostic agent operations
  - **Property 13: Operating system-agnostic worker agent operations**
  - **Validates: Requirements 7.4**

- [ ] 7. Update concrete worker classes and add property tests
  - Update `WindowsInstanceBuildWorker` to work with new architecture
  - Update `PosixInstanceBuildWorker` to work with new architecture
  - Ensure `configure_worker_command()` methods work with WorkerHost delegation
  - **Simultaneously update any affected tests to maintain passing status**
  - Write property test for sequential configuration (**Property 3: Sequential worker agent configuration**)
  - Write property test for configuration correctness (**Property 4: Worker agent configuration correctness**)
  - _Requirements: 7.1, 7.2, 7.3_
  - _Validates: Requirements 1.2, 1.4, 2.5, 5.4, 1.3, 4.3, 5.3_

- [ ] 8. Checkpoint - Verify all tests still pass
  - Run full test suite to verify no regressions were introduced
  - All tests should already be passing due to simultaneous test updates in previous tasks

## Phase 3: Update File Mappings and Error Handling

- [ ] 9. Move file mappings to worker agent lifecycle and add property test
  - Move file staging from `_launch_instance()` to worker agent `start()` method
  - Implement file cleanup in worker agent `stop()` method
  - Update file transfer to happen after host is running but before agent configuration
  - Write property test for file mappings cleanup (**Property 9: Worker agent state cleanup**)
  - **Simultaneously update any affected tests to maintain passing status**
  - _Requirements: 5.1, 5.2_
  - _Validates: Requirements 6.2_

- [ ] 10. Implement enhanced error handling and add property tests
  - Create `WorkerHostError` and `WorkerAgentError` exception classes
  - Update WorkerHost methods to raise host-specific errors with diagnostics
  - Update DeadlineWorker methods to raise agent-specific errors with logs
  - Ensure error messages distinguish between host and agent failures
  - Write property test for error diagnostics (**Property 11: Error diagnostics inclusion**)
  - Write property test for error source distinction (**Property 12: Error source distinction**)
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5_
  - _Validates: Requirements 6.1, 6.2, 6.3, 6.4, 6.5_

## Phase 4: Update Fixtures and Integration

- [ ] 11. Create worker_host fixture
  - Create new `worker_host` pytest fixture that provides EC2WorkerHost without agent
  - Implement OS detection and appropriate WorkerHost creation
  - Add proper lifecycle management (start on setup, stop on teardown)
  - _Requirements: 3.1, 3.2, 3.3, 3.4_

- [ ] 12. Update existing worker fixture
  - Update `worker` fixture to depend on `worker_host` fixture
  - Modify worker creation to use explicit WorkerHost composition
  - Ensure backward compatibility for existing tests
  - _Requirements: 3.5_

- [ ] 13. Update DeadlineWorkerConfiguration for host reuse and add property test
  - Ensure file_mappings behavior supports host reuse scenarios
  - Update configuration validation for new architecture
  - Write property test for configuration acceptance (**Property 20: Configuration acceptance**)
  - _Requirements: 5.1, 5.3, 5.5_
  - _Validates: Requirements 5.1_

## Phase 5: Integration Testing and Validation

- [ ] 14. Create integration tests for host reuse and add property test
  - Write integration test that creates one host and uses it with multiple worker configurations
  - Test that worker agent state is properly cleaned up between configurations
  - Test that different file mappings work correctly on reused hosts
  - Write property test for host reuse independence (**Property 14: Host reuse independence**)
  - _Requirements: 1.1, 1.2, 1.3, 1.4_
  - _Validates: Requirements 1.2, 2.4, 4.4_

- [ ] 15. Create integration tests for error scenarios
  - Test error handling when host operations fail
  - Test error handling when agent operations fail
  - Verify error messages contain appropriate diagnostic information
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5_

- [ ] 16. Validate operating system support preservation and add property test
  - Run existing tests with new architecture to ensure no regressions
  - Test both Windows and POSIX worker functionality
  - Verify all existing worker capabilities are preserved
  - Write property test for OS support preservation (**Property 21: Operating system support preservation**)
  - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5_
  - _Validates: Requirements 7.1, 7.2, 7.3, 7.4_

- [ ] 17. Create property-based tests for worker agent service registration
  - Test that reconfiguring workers creates new worker IDs and deletes old ones
  - Test worker ID management across different configurations
  - Write property test for service registration (**Property 10: Worker agent service registration**)
  - _Requirements: 5.5_
  - _Validates: Requirements 5.5_

## Phase 6: Documentation and Migration Support

- [ ] 18. Update API documentation
  - Update docstrings for all refactored classes and methods
  - Add examples showing host reuse patterns
  - Document breaking changes and migration requirements
  - _Requirements: All_

- [ ] 19. Create migration examples
  - Create example code showing old vs new patterns
  - Document fixture usage patterns for host reuse
  - Add examples for different scoped fixtures (function, class, module)
  - _Requirements: All_

- [ ] 20. Validate backward compatibility where possible
  - Ensure existing tests continue to work with minimal changes
  - Verify that the `worker` fixture maintains its existing behavior
  - Test that DockerContainerWorker remains unchanged
  - _Requirements: 7.5_