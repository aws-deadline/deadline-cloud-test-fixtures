## 0.18.21 (2026-09-01)

### Features
* Added `LocalMacWorker` for running macOS Deadline workers directly on the test host instead of provisioning a separate machine. This also adds a `MACOS` variant to `OperatingSystem`, enabling macOS-based test workflows. (#329)
## 0.18.20 (2026-08-26)

### Bug Fixes
* Fixed GUI test fixtures to use the new "Save bundle as" button instead of the renamed "Export Bundle" button. (#322)
## 0.18.19 (2026-08-18)
## 0.18.18 (2026-07-28)

### Features
* Added `session_runtime` field to `DeadlineWorkerConfiguration`, allowing you to configure the OpenJD session runtime backend ("python", "rust", or "service-selected") when deploying workers in E2E tests. This is supported on both Linux and Windows workers. (`85ee7b0`)
## 0.18.17 (2026-07-17)

### Bug Fixes
* Fixed an issue with threaded mock server startup that could cause test fixture initialization failures. (#303)
## 0.18.16 (2026-07-17)

### Features
* Added test fixtures for GUI testing using xa11y, enabling automated accessibility-based GUI testing. (#301)
## 0.18.15 (2026-07-07)

### Features
* Updated the default Python version for Windows worker agent to 3.13.14. (#296)
## 0.18.14 (2026-06-30)

### Bug Fixes
* Fixed an issue where old job attachment queues were not being properly cleaned up during tests. (#280)
## 0.18.13 (2026-06-15)

### Bug Fixes
* The configure command now retries once if it fails, improving reliability during test setup. (#282)
## 0.18.12 (2026-05-26)

### Bug Fixes
* EC2 worker bootstrap pip install commands now include `--retries 10 --timeout 60` to handle transient CodeArtifact failures (e.g., 504 errors). This applies to both the pip upgrade step and the main install step in `PipInstall.install_command_for_linux` and `PipInstall.install_command_for_windows`. (#277)
## 0.18.11 (2026-04-13)

### Bug Fixes
* Update Windows Python version for worker agent to Python 3.13.13 (#267) ([`50244ee`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/50244ee5ecbbd58458ddd2ebbdd0aebe7a62ec62))


## 0.18.10 (2026-01-22)


### Bug Fixes
* Unintentional cleanup of leftover JobAttachmentManager queues from other concurrent test runs (#259) ([`4d69e8d`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/4d69e8dd175a2cbb6cc88dd34d004c4039080a16))


## 0.18.9 (2026-01-19)


### Features
* provide importable hook for cleaning up leftover JobAttachmentManager queues (#256) ([`51c6f92`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/51c6f9297c58eec1ec180c16bcc0b53fde2f759e))



## 0.18.8 (2025-12-08)


### Features
* Check if userdata finishes successfully (#240) ([`dec2a8f`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/dec2a8f5c7b052ffbff90410dd5323ab57845d54))



## 0.18.7 (2025-10-15)



### Bug Fixes
* &#34;no latest session action ID&#34; error after job completed (#218) ([`5f7ff02`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/5f7ff02aaea328e0f1f911257586140f4429f295))

## 0.18.6 (2025-10-08)


### Features
* Add ability to pass env var to worker config (#214) ([`8402cf8`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/8402cf8e8772c204cf3da61ca52563b6932a1665))


## 0.18.5 (2025-10-03)

### Features
* add ability to tag instances (#210) ([`ae42847`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/ae42847375278f1fb79668db65de93b33610c250))


## 0.18.4 (2025-08-18)


### Features
* add additional debug logging when SSM waiter fails (#201) ([`1bc786d`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/1bc786de6e833756363c0f345f273a0f7797add4))

### Bug Fixes
* SSM waiter timeouts not long enough for Windows worker agent setup (#202) ([`61988aa`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/61988aa5c61ec66acc9c0b6d1fcda60d58a7e175))


## 0.18.3 (2025-07-17)


### Features
* Retry on SSM Command undeliverable (#196) ([`f853231`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/f8532319c6f44b1356a59d93ca2f16044bec6a28))


## 0.18.2 (2025-06-16)



### Bug Fixes
* errors while trying to stop queue fleet associations that are already stopped/ing (#194) ([`76540f4`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/76540f46439ba4d0086a9cf14600422d4d456be4))

## 0.18.1 (2025-05-20)


### Features
* configurable fleet active timeout in Fleet.create() (#191) ([`c66d883`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/c66d883d9bc14032a6643cf5194731c6446e222e))


## 0.18.0 (2025-05-16)

### BREAKING CHANGES
* improved exception handling when instance startup fails. DescribeInstances and DescribeInstanceStatus are now called by the test runner, IAM roles will need to be updated accordingly. (#189) ([`d2b9d4c`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/d2b9d4c7a24b2b9a2074b7da161a3c2c99eed024))



## 0.17.6 (2025-04-30)


### Features
* Allow providing agent user credentials as a secrets manager secret (#187) ([`e49691c`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/e49691cd80d0c09a2bd949f9dac00362b9c3e387))
* Add configurable timeout lengths to send_command ([`18c533b`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/18c533b79d9c4ae79fdd6b12713f5d86285f9798))


## 0.17.5 (2025-04-04)


### Features
* Add support to fetch and match worker logs (#183) ([`b920331`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/b9203319b189cd957543c78549df5c1d0ee7d4e0))
* append additional configuration to deadline-worker systemd config during linux worker setup (#181) ([`13d60e1`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/13d60e1d504c72d6df6629b5e2948f0bfc10408f))


## 0.17.4 (2025-01-27)


### Features
* ability to supply deadline Python wheel (#174) ([`13662a4`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/13662a4c2b567ccae24acd758b20657824d1f79c))


## 0.17.3 (2025-01-09)


### Features
* configurable worker session root directory (#170) ([`0d20e33`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/0d20e33d695af0d174412ab1dc8343898f1b98af))

### Bug Fixes
* linux instance has unused ebs block device (#172) ([`50bfe43`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/50bfe43e5819011896e47140926b9d70d7811eb5))

## 0.17.2 (2024-12-14)



### Bug Fixes
* increase disk size temporarily to allow more than 4GB of space on windows (#168) ([`7c33f67`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/7c33f67ec3c88009591b81995d5b956eba7f8d68))

## 0.17.1 (2024-11-21)


### Features
* ability to supply openjd-sessions Python wheel (#165) ([`df45370`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/df453704dad2b7b00ec0573993396cbbe99eb9d1))

### Bug Fixes
* incorrect filesystem paths to files fetched from S3 by userdata (#164) ([`5a6c0ee`](https://github.com/aws-deadline/deadline-cloud-test-fixtures/commit/5a6c0ee76934e1bf06a25e39c285633336ccafb9))

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

