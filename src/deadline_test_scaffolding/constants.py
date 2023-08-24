# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
import os

STAGE = os.environ.get("STAGE", "Prod")

BOOTSTRAP_CLOUDFORMATION_STACK_NAME = f"TestScaffoldingStack{STAGE}"

# Role Names
DEADLINE_WORKER_BOOTSTRAP_ROLE = f"DeadlineWorkerBootstrapRole{STAGE}"
DEADLINE_WORKER_BOOSTRAP_INSTANCE_PROFILE_NAME = f"DeadlineWorkerBootstrapInstanceProfile{STAGE}"
DEADLINE_WORKER_ROLE = f"DeadlineWorkerTestRole{STAGE}"
DEADLINE_QUEUE_SESSION_ROLE = f"DeadlineScaffoldingQueueSessionRole{STAGE}"

# Job Attachments
JOB_ATTACHMENTS_BUCKET_RESOURCE = "ScaffoldingJobAttachmentsBucket"
JOB_ATTACHMENTS_BUCKET_NAME = os.environ.get(
    "JOB_ATTACHMENTS_BUCKET_NAME", "scaffolding-job-attachments-bucket"
)
JOB_ATTACHMENTS_BUCKET_POLICY_RESOURCE = f"JobAttachmentsPolicy{STAGE}"
JOB_ATTACHMENTS_ROOT_PREFIX = "root"

# Worker Agent Configurations
DEFAULT_CMF_CONFIG = {
    "customerManaged": {
        "autoScalingConfiguration": {
            "mode": "NO_SCALING",
            "maxFleetSize": 1,
        },
        "workerRequirements": {
            "vCpuCount": {"min": 1},
            "memoryMiB": {"min": 1024},
            "osFamily": "linux",
            "cpuArchitectureType": "x86_64",
        },
    }
}

# Service Principals
CREDENTIAL_VENDING_PRINCIPAL = os.environ.get(
    "CREDENTIAL_VENDING_PRINCIPAL", "credential-vending.deadline-closed-beta.amazonaws.com"
)

# Temporary constants
DEADLINE_SERVICE_MODEL_BUCKET = os.environ.get("DEADLINE_SERVICE_MODEL_BUCKET", "")
CODEARTIFACT_DOMAIN = os.environ.get("CODEARTIFACT_DOMAIN", "")
CODEARTIFACT_ACCOUNT_ID = os.environ.get("CODEARTIFACT_ACCOUNT_ID", "")
CODEARTIFACT_REPOSITORY = os.environ.get("CODEARTIFACT_REPOSITORY", "")
