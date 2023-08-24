# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

MOCK_FARM_ID = "farm-0123456789abcdefabcdefabcdefabcd"
MOCK_FARM_NAME = "fake_farm_name"
MOCK_FLEET_ID = "fleet-0123456789abcdefabcdefabcdefabcd"
MOCK_FLEET_NAME = "fake_fleet_name"
MOCK_QUEUE_ID = "queue-0123456789abcdefabcdefabcdefabcd"
MOCK_QUEUE_NAME = "fake_queue_name"
MOCK_WORKER_ROLE_ARN = "fake_worker_role_arn"
MOCK_JOB_ATTACHMENTS_BUCKET_NAME = "fake_job_attachments_bucket_name"

MOCK_DEFAULT_CMF_CONFIG = {
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
