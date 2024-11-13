## 0.17.0 (2024-11-13)


### Features
* allow worker config tests to disallow instance profile (#162) ([`db6f441`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/db6f441c30d6b8d9070ebe5001d7a67960c7a3c5))


## 0.16.0 (2024-10-30)

### BREAKING CHANGES
* The default of `DeadlineWorkerConfiguration.start_service` was changed from `False` &rarr; `True`
* Downstream consumers of this package may have relied on a bug where the worker agent being started through an SSM command (see #160)

### Bug Fixes
* concurrent worker agent processes (#160) ([`9f7da7f`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/9f7da7f6873f0f25c7f67b34de7f1e28b85dc947))



## 0.15.0 (2024-10-21)

### BREAKING CHANGES
* increase retries for log contains utils  (#156) ([`3616a9b`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/3616a9b4da1ed98896e7a5a1bc34a0dc0e3dbd05))

### Features
* allow local session logs to be turned off in worker tests (#155) ([`656db75`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/656db7546325c013b52e02c360a8b81ecb6905b3))
* Added negative log assertions (#154) ([`d75f580`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/d75f5807ca9235491bcbf43d0306048914f09d17))

### Bug Fixes
* add WaiterConfig to ssm command waiter (#152) ([`65b93ea`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/65b93eab837387e2776e427d273dbbeba8d73ef7))

## 0.14.0 (2024-09-05)

### BREAKING CHANGES
* `Job.lifecycle_status` changed from `str` &rarr; `deadline_cloud_test_fixtures.deadline.JobLifecycleStatus`
* `deadline_cloud_test_fixtures.TaskStatus.UNKNOWN` removed

### Features
* job/step/task API functions and session log assertions (#150) ([`18f7078`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/18f7078441e7d3fe02efb51f1175aa1595ab2df3))



## 0.13.2 (2024-09-04)



### Bug Fixes
* silent progress ui (#148) ([`5df19d7`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/5df19d70754e768db9189a29612803d3acf30015))

## 0.13.1 (2024-08-26)



### Bug Fixes

* start service after running the installer instead of during (#146) ([`adac342`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/adac342fc6dff6c6174ae39783448b1020cc9a61))

## 0.13.0 (2024-08-16)

### BREAKING CHANGES
* improve support for ec2 instance workers (#143) ([`afbc5fb`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/afbc5fb88f981976d60635cfb84d409eda65ffb8))


### Bug Fixes
* start windows service depending on the worker configuration (#144) ([`602bd3b`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/602bd3b511b048db418734fc1a29ed43fd0ba6b2))

## 0.12.2 (2024-08-14)



### Bug Fixes
* append export AWS_ENDPOINT_URL_DEADLINE at end of worker SSM commands (#141) ([`cfec139`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/cfec1399ad7a0c0d25b880d68d58a0ad1814a68b))
* increase number of retries for SSM send_command to 60 (#140) ([`e768f38`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/e768f38a65eb10a728b298eae93929a2784bd80c))

## 0.12.1 (2024-08-13)


### Features
* linux ec2-based tests test installer-based user creation (#138) ([`8cf1ffb`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/8cf1ffb3fc054370adfa357ffe6657e928263a1e))

### Bug Fixes
* add missing comma to ssm command (#137) ([`b8db187`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/b8db1872411ac28fe7273dda31275c2812bac0e1))
* remove runas from job attachment fixture (#136) ([`28767c7`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/28767c7bf1a87bab9e4ae75997bb74f21e2fe3e4))
* Add default max_retries to Job.wait_until_complete to avoid infinite waiting (#134) ([`6d4023d`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/6d4023d244225a68d282839d60d342138a9e25d2))
* resolve get_worker_id race by waiting for worker.json to get written (#133) ([`1f27578`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/1f27578da0f2fe9cc241334acd439d9e1d741a1d))

## 0.12.0 (2024-07-18)

### BREAKING CHANGES
* Add stop/start worker agent service method (#130) ([`0ea4b3c`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/0ea4b3cff9da04e4573b57a06229812b79e07ced))



## 0.11.0 (2024-07-15)

### BREAKING CHANGES
* Refactoring EC2InstanceWorker to Split out PosixInstanceWorker and WindowsInstanceWorker (#125) ([`5705df4`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/5705df43bfebf86653858288bc3121e6a1b5bef7))


### Bug Fixes
* BYO Deadline now looks specifically for resource env vars. (#128) ([`852fef3`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/852fef32c3ed42ae327120ff1a3d90fe4478d2a6))

## 0.10.0 (2024-07-04)

### BREAKING CHANGES
* delete workers from non-autoscaling fleets (#124) ([`1217b4b`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/1217b4b91e06ad3dc7f26c273bf98a72d7bf00fe))



## 0.9.0 (2024-06-26)

### BREAKING CHANGES
* Refactor JA integ test resource creation so that we use some resources from the environment (#119) ([`50b36f1`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/50b36f10d38b60f5c5d9aecd88ab846a3fe4cba8))



## 0.8.1 (2024-06-24)



### Bug Fixes
* check value of operating_system.name instead of operating_system (#117) ([`094069d`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/094069d92863fbc7c1c2f0cf61647ae9fc8622df))

## 0.8.0 (2024-06-17)

### BREAKING CHANGES
* add windows support to worker fixture (#115) ([`ef7f133`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/ef7f1336d6c489982ed18cd11279faa0699c460c))



## 0.7.1 (2024-06-07)



### Bug Fixes
* restore support for deprecated WORKER_REGION env var (#113) ([`58cc193`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/58cc19315285fec9ad9651ec7b65066d83e7b1dd))

## 0.7.0 (2024-06-06)

### BREAKING CHANGES
* set AWS_ENDPOINT_URL_DEADLINE after installing service model (#96) ([`6bc4d8f`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/6bc4d8f024aed18c68fa207c4e01ecfbc7a6edd6))



## 0.6.2 (2024-04-02)



### Bug Fixes
* stop using removed worker agent CLI argument (#93) ([`f831b92`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/f831b921aa1090f175466e84c9f2d192ae275533))

## 0.6.1 (2024-04-01)

### CI
* fixed ci publishing issue 


## 0.6.0 (2024-04-01)

### BREAKING CHANGES
* public release (#83) ([`e114779`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/e1147791d2a80ea60acb2f18eff9de350756ab59))



## 0.5.6 (2024-03-24)



### Bug Fixes
* FleetAPI compatibility for WorkerRequirements (#80) ([`1f3978b`](https://github.com/casillas2/deadline-cloud-test-fixtures/commit/1f3978b96b0f5a4a46586f089dea44afdcc5c877))

## 0.5.5 (2024-03-12)



### Bug Fixes
* Tests failing as new Task Status was added to API (#68) ([`a1ea341`](https://github.com/casillas2/deadline-cloud-test-fixtures/commit/a1ea3411f9683d83a4aa90b22f03b5893f847159))

