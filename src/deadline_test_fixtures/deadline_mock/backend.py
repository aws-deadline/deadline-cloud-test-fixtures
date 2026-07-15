# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Observable HTTP mock for Deadline submitter tests."""

from __future__ import annotations

import json
import re
import sys
import threading
import time
import traceback
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Optional
from urllib.parse import parse_qs, urlparse

import botocore.session
from botocore.exceptions import ClientError
from botocore.model import ServiceModel
from botocore.validate import ParamValidator

from .scenario import MockDeadlineScenario

API_PREFIX = "/2023-10-12"
ADMIN_STATE_PATH = "/__deadline_test_fixtures__/state"
ADMIN_RESET_PATH = "/__deadline_test_fixtures__/reset"

_INT_QUERY_PARAMS = {"maxResults", "itemOffset", "pageSize"}
_MAX_BODY_BYTES = 10 * 1024 * 1024


def route(method: str, path: str, operation: str) -> Callable:
    """Associate a backend method with a Deadline REST-JSON route."""

    def decorator(function: Callable) -> Callable:
        setattr(function, "__http_route__", (method, f"{API_PREFIX}{path}", operation))
        return function

    return decorator


def _resource_not_found(resource_type: str, resource_id: str, operation: str) -> ClientError:
    return ClientError(
        {
            "Error": {
                "Code": "ResourceNotFoundException",
                "Message": (
                    f"Resource of type {resource_type} with id {resource_id} does not exist."
                ),
            }
        },
        operation,
    )


class MockDeadlineBackend:
    """In-memory resources and observability for the mock service."""

    def __init__(
        self,
        scenario: Optional[MockDeadlineScenario] = None,
        *,
        validate_responses: bool = True,
    ) -> None:
        self.scenario = scenario or MockDeadlineScenario()
        self.validate_responses = validate_responses
        self.response_delay_s = self.scenario.response_delay_s
        self._lock = threading.Lock()
        self.log_callback: Optional[Callable[[str], None]] = None
        self.farms: dict[str, dict[str, Any]] = {}
        self.queues: dict[tuple[str, str], dict[str, Any]] = {}
        self.queue_environments: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self.storage_profiles: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self.call_counts: dict[str, int] = {}
        self.request_log: list[tuple[str, str, str]] = []
        self.unmatched_requests: list[tuple[str, str]] = []
        self.scenario.seed(self)

    @property
    def farm_id(self) -> str:
        return self.scenario.farm_id

    @property
    def queue_id(self) -> str:
        return self.scenario.queue_id

    def reset(self) -> None:
        """Restore scenario resources and clear request observability."""
        with self._lock:
            self.call_counts.clear()
            self.request_log.clear()
            self.unmatched_requests.clear()
            self.scenario.seed(self)

    def snapshot(self) -> dict[str, Any]:
        """Return JSON-serializable state for remote assertions."""
        with self._lock:
            return {
                "identifiers": {
                    "farm_id": self.farm_id,
                    "queue_id": self.queue_id,
                },
                "call_counts": dict(self.call_counts),
                "request_log": [list(request) for request in self.request_log],
                "unmatched_requests": [list(request) for request in self.unmatched_requests],
                "resources": {
                    "farms": list(self.farms.values()),
                    "queues": list(self.queues.values()),
                    "queue_environments": [
                        environment
                        for environments in self.queue_environments.values()
                        for environment in environments
                    ],
                    "storage_profiles": [
                        profile
                        for profiles in self.storage_profiles.values()
                        for profile in profiles
                    ],
                },
            }

    def _log(self, message: str) -> None:
        if self.log_callback is not None:
            try:
                self.log_callback(message)
            except Exception:
                # Observability is best-effort and must not break request handling.
                pass

    def _queue_key(self, farm_id: str, queue_id: str, operation: str) -> tuple[str, str]:
        key = (farm_id, queue_id)
        if key not in self.queues:
            raise _resource_not_found("queue", queue_id, operation)
        return key

    @route("GET", "/farms", "ListFarms")
    def list_farms(self, **kwargs: Any) -> dict[str, Any]:
        return {"farms": list(self.farms.values())}

    @route("GET", "/farms/{farmId}", "GetFarm")
    def get_farm(self, *, farmId: str) -> dict[str, Any]:
        try:
            return dict(self.farms[farmId])
        except KeyError:
            raise _resource_not_found("farm", farmId, "GetFarm")

    @route("GET", "/farms/{farmId}/queues", "ListQueues")
    def list_queues(self, *, farmId: str, **kwargs: Any) -> dict[str, Any]:
        if farmId not in self.farms:
            raise _resource_not_found("farm", farmId, "ListQueues")
        return {
            "queues": [
                queue
                for (candidate_farm, _), queue in self.queues.items()
                if candidate_farm == farmId
            ]
        }

    @route("GET", "/farms/{farmId}/queues/{queueId}", "GetQueue")
    def get_queue(self, *, farmId: str, queueId: str) -> dict[str, Any]:
        key = self._queue_key(farmId, queueId, "GetQueue")
        return dict(self.queues[key])

    @route(
        "GET",
        "/farms/{farmId}/queues/{queueId}/storage-profiles",
        "ListStorageProfilesForQueue",
    )
    def list_storage_profiles_for_queue(
        self, *, farmId: str, queueId: str, **kwargs: Any
    ) -> dict[str, Any]:
        key = self._queue_key(farmId, queueId, "ListStorageProfilesForQueue")
        return {"storageProfiles": list(self.storage_profiles.get(key, ()))}

    @route(
        "GET",
        "/farms/{farmId}/queues/{queueId}/environments",
        "ListQueueEnvironments",
    )
    def list_queue_environments(
        self, *, farmId: str, queueId: str, **kwargs: Any
    ) -> dict[str, Any]:
        key = self._queue_key(farmId, queueId, "ListQueueEnvironments")
        return {"environments": list(self.queue_environments.get(key, ()))}

    @route(
        "GET",
        "/farms/{farmId}/queues/{queueId}/environments/{queueEnvironmentId}",
        "GetQueueEnvironment",
    )
    def get_queue_environment(
        self,
        *,
        farmId: str,
        queueId: str,
        queueEnvironmentId: str,
    ) -> dict[str, Any]:
        key = self._queue_key(farmId, queueId, "GetQueueEnvironment")
        for environment in self.queue_environments.get(key, ()):
            if environment.get("queueEnvironmentId") == queueEnvironmentId:
                return dict(environment)
        raise _resource_not_found("queueEnvironment", queueEnvironmentId, "GetQueueEnvironment")


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Type {type(value)} is not JSON serializable")


def _discover_routes(backend: MockDeadlineBackend) -> list[tuple[str, re.Pattern, Callable, str]]:
    routes = []
    for name in dir(backend):
        function = getattr(backend, name)
        route_info = getattr(function, "__http_route__", None)
        if route_info is None:
            continue
        method, path, operation = route_info
        pattern = re.compile("^" + re.sub(r"\{(\w+)\}", r"(?P<\1>[^/]+)", path) + "$")
        routes.append((method, pattern, function, operation))
    return routes


class _ResponseValidator:
    """Filter responses to the installed botocore model, then validate them."""

    _TYPE_DEFAULTS = {
        "string": "",
        "integer": 0,
        "long": 0,
        "float": 0.0,
        "double": 0.0,
        "boolean": False,
        "timestamp": datetime(1970, 1, 1, tzinfo=timezone.utc),
        "list": [],
        "map": {},
        "structure": {},
    }

    def __init__(self) -> None:
        session = botocore.session.get_session()
        loader = session.get_component("data_loader")
        self._model = ServiceModel(loader.load_service_model("deadline", "service-2"))
        self._validator = ParamValidator()

    def _filter(self, shape: Any, value: Any) -> Any:
        if shape is None or value is None:
            return value
        if shape.type_name == "structure":
            filtered = {
                key: self._filter(shape.members[key], child)
                for key, child in value.items()
                if key in shape.members
            }
            for required in getattr(shape, "required_members", ()):
                if required not in filtered and required in shape.members:
                    filtered[required] = self._TYPE_DEFAULTS.get(shape.members[required].type_name)
            return filtered
        if shape.type_name == "list":
            return [self._filter(shape.member, child) for child in value]
        if shape.type_name == "map":
            return {key: self._filter(shape.value, child) for key, child in value.items()}
        return value

    def filter_and_validate(self, operation_name: str, response: dict[str, Any]) -> dict[str, Any]:
        output_shape = self._model.operation_model(operation_name).output_shape
        if output_shape is None:
            return response
        filtered = self._filter(output_shape, response)
        report = self._validator.validate(filtered, output_shape)
        if report.has_errors():
            raise ValueError(
                f"Mock response for {operation_name} failed validation: "
                f"{report.generate_report()}"
            )
        return filtered


def _make_handler(
    routes: list[tuple[str, re.Pattern, Callable, str]],
    validator: Optional[_ResponseValidator],
    backend: MockDeadlineBackend,
) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            return

        def _send_json(self, status: int, body: Any, *, error_code: Optional[str] = None) -> None:
            payload = json.dumps(body, default=_json_default).encode("utf-8")
            try:
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                if error_code:
                    self.send_header("x-amzn-errortype", error_code)
                self.end_headers()
                self.wfile.write(payload)
            except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
                # DCC clients can abandon in-flight requests while closing or reloading.
                pass

        def handle_one_request(self) -> None:
            try:
                super().handle_one_request()
            except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
                self.close_connection = True

        def _dispatch(self, method: str) -> None:
            parsed = urlparse(self.path)
            if method == "GET" and parsed.path == ADMIN_STATE_PATH:
                self._send_json(200, backend.snapshot())
                return
            if method == "POST" and parsed.path == ADMIN_RESET_PATH:
                backend.reset()
                self._send_json(200, backend.snapshot())
                return

            for route_method, pattern, function, operation in routes:
                match = pattern.match(parsed.path)
                if route_method != method or match is None:
                    continue
                if backend.response_delay_s:
                    time.sleep(backend.response_delay_s)
                with backend._lock:
                    backend.call_counts[operation] = backend.call_counts.get(operation, 0) + 1
                    backend.request_log.append((method, parsed.path, operation))
                try:
                    arguments: dict[str, Any] = dict(match.groupdict())
                    for key, values in parse_qs(parsed.query).items():
                        arguments[key] = int(values[0]) if key in _INT_QUERY_PARAMS else values[0]
                    length = int(self.headers.get("Content-Length", 0))
                    if length > _MAX_BODY_BYTES:
                        self._send_json(413, {"message": "Payload too large"})
                        return
                    if length:
                        body = json.loads(self.rfile.read(length))
                        if not isinstance(body, dict):
                            raise ValueError("Request body must be a JSON object")
                        arguments.update(body)
                except (TypeError, ValueError) as error:
                    self._send_json(
                        400,
                        {"message": f"Invalid request: {error}"},
                        error_code="ValidationException",
                    )
                    return
                try:
                    result = function(**arguments)
                    if validator is not None:
                        result = validator.filter_and_validate(operation, result)
                    backend._log(f"served {operation} ({method} {parsed.path}) -> 200")
                    self._send_json(200, result)
                except ClientError as error:
                    details = error.response["Error"]
                    code = details.get("Code", "InternalServerException")
                    status = 404 if code == "ResourceNotFoundException" else 400
                    self._send_json(
                        status,
                        {"message": details.get("Message", "")},
                        error_code=code,
                    )
                except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
                    self.close_connection = True
                except Exception as error:
                    traceback.print_exc()
                    try:
                        self._send_json(
                            500,
                            {"message": str(error)},
                            error_code="InternalServerException",
                        )
                    except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
                        self.close_connection = True
                return

            with backend._lock:
                backend.unmatched_requests.append((method, parsed.path))
            backend._log(f"UNMATCHED {method} {parsed.path} -> 404")
            sys.stderr.write(f"[mock-deadline] 404 NO ROUTE {method} {parsed.path}\n")
            sys.stderr.flush()
            self._send_json(
                404,
                {"message": f"No route for {method} {parsed.path}"},
                error_code="NotFoundException",
            )

        def do_GET(self) -> None:  # noqa: N802
            self._dispatch("GET")

        def do_POST(self) -> None:  # noqa: N802
            self._dispatch("POST")

        def do_PATCH(self) -> None:  # noqa: N802
            self._dispatch("PATCH")

    return Handler


def start_server(
    backend: MockDeadlineBackend,
    port: int = 0,
) -> tuple[ThreadingHTTPServer, str, threading.Thread]:
    """Serve a mock Deadline backend over REST-JSON."""
    routes = _discover_routes(backend)
    validator = _ResponseValidator() if backend.validate_responses else None
    server = ThreadingHTTPServer(
        ("127.0.0.1", port),
        _make_handler(routes, validator, backend),
    )
    actual_port = int(server.server_address[1])
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://127.0.0.1:{actual_port}", thread
