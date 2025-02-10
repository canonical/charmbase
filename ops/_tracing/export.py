# Copyright 2022 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
# file except in compliance with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under
# the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific language
# governing permissions and limitations under the License.
"""FIXME Docstring."""

from __future__ import annotations

import contextlib
import logging
import os
import threading
import time
from typing import Sequence

from opentelemetry.exporter.otlp.proto.common._internal import trace_encoder
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.urllib import URLLibInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter, SpanExportResult
from opentelemetry.trace import get_tracer_provider, set_tracer_provider

import ops
import ops._tracing.buffer
import ops.jujucontext
import ops.log

# Trace `urllib` usage when talking to Pebble
URLLibInstrumentor().instrument()

# NOTE: nominally int, although float would work just as well in practice
EXPORTER_TIMEOUT: int = 1  # seconds
"""How much to give OTLP span exporter has to push traces to the backend."""

SENDOUT_FACTOR: int = 2
"""How many buffered chunks to send out for each incoming chunk."""

BUFFER_FILE: str = '.tracing-data.db'
"""Name of the file whither data is buffered, located next to .unit-state.db."""


logger = logging.getLogger(__name__)
_exporter: ProxySpanExporter | None = None


# NOTE: OTEL SDK suppresses errors while exporting data
# TODO: decide if we need to remove this before going to prod
logger.addHandler(logging.StreamHandler())


class ProxySpanExporter(SpanExporter):
    real_exporter: OTLPSpanExporter | None = None
    settings: tuple[str | None, str | None] = (None, None)

    def __init__(self, buffer_path: str):
        self.buffer = ops._tracing.buffer.Buffer(buffer_path)
        self.lock = threading.Lock()

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        """Export a batch of telemetry data.

        Note: to avoid data loops or recursion, this function cannot be instrumented.
        """
        try:
            with suppress_juju_log_handler():
                # Note:
                # this is called in a helper thread, which is daemonic,
                # the MainThread will wait at most 10s for this thread.
                # Margins:
                # - 1s safety margin
                # - 1s for buffered data time overhang
                # - 2s for live data
                deadline = time.monotonic() + 6

                assert spans  # the BatchSpanProcessor won't call us if there's no data
                # TODO:  this will change in the JSON experiment
                data: bytes = trace_encoder.encode_spans(spans).SerializePartialToString()
                rv = self.buffer.pump(data)
                assert rv
                self.do_export(*rv)

                for _ in range(SENDOUT_FACTOR - 1):
                    if time.monotonic() > deadline:
                        break
                    if not (rv := self.buffer.pump()):
                        break
                    self.do_export(*rv)

                return SpanExportResult.SUCCESS
        except Exception:
            logger.exception('export')
            raise

    def do_export(self, buffered_id: int, data: bytes) -> None:
        """Export buffered data and remove it from the buffer on success."""
        # TODO:  this will change in the JSON experiment
        exporter = self.real_exporter
        if exporter and exporter._export(data).ok:
            self.buffer.remove(buffered_id)

    def shutdown(self) -> None:
        """Shut down the exporter."""
        if exporter := self.real_exporter:
            exporter.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        """No-op, as the real exporter doesn't buffer."""
        return True


@contextlib.contextmanager
def suppress_juju_log_handler():
    handlers = [h for h in logging.root.handlers if isinstance(h, ops.log.JujuLogHandler)]
    if not handlers:
        yield
        return

    juju_log_handler = handlers[0]
    token = juju_log_handler.drop.set(True)
    try:
        yield
    finally:
        juju_log_handler.drop.reset(token)


def setup_tracing(charm_class_name: str) -> None:
    global _exporter
    # FIXME would it be better to pass Juju context explicitly?
    juju_context = ops.jujucontext._JujuContext.from_dict(os.environ)
    app_name = '' if juju_context.unit_name is None else juju_context.unit_name.split('/')[0]
    service_name = f'{app_name}-charm'  # only one COS charm sets custom value

    resource = Resource.create(
        attributes={
            # https://opentelemetry.io/docs/languages/sdk-configuration/general/
            # https://github.com/open-telemetry/semantic-conventions/tree/main/docs/resource#semantic-attributes-with-dedicated-environment-variable
            #
            # OTEL defines some standard-ish attributes:
            # service.name        required
            # service.instance.id recommended
            # service.namespace   recommended -- maybe model name?
            # service.version     recommended
            # Following same attribute names as charm_tracing lib
            # FIXME: decide if it makes sense
            'service.name': service_name,
            'compose_service': service_name,  # FIXME why is this copy needed?
            'charm_type': charm_class_name,
            # juju topology
            'juju_unit': juju_context.unit_name,
            'juju_application': app_name,
            'juju_model': juju_context.model_name,
            'juju_model_uuid': juju_context.model_uuid,
        }
    )
    provider = TracerProvider(resource=resource)

    # How

    buffer_path = str(juju_context.charm_dir / BUFFER_FILE)
    _exporter = ProxySpanExporter(buffer_path)
    span_processor = BatchSpanProcessor(_exporter)
    provider.add_span_processor(span_processor)
    set_tracer_provider(provider)


def set_tracing_destination(
    *,
    url: str | None,
    ca: str | None,
) -> None:
    # FIXME only if it's a path, obv...
    # should we also check that this path exists?
    if ca is not None and not ca.startswith('/'):
        raise ValueError(f'{ca=} must be an absolute path')

    assert _exporter, 'tracing has not been set up'
    with _exporter.lock:
        if (url, ca) != _exporter.settings:
            if url:
                # real exporter, hardcoded for now
                real_exporter = OTLPSpanExporter(url, timeout=EXPORTER_TIMEOUT)
                # FIXME: shouldn't be hardcoded...
                # FIXME API design: if it OK to force the protocol and endpoint
                # switch onto the charmers, our users?
                #
                # OTLP protobuf URL is  host:4318/v1/traces
                # Zipkin v2 JSON URL is host:9411/api/v2/spans
                #
                # FIXME: on the other hand, Jaeger 2 should accept OTLP JSON too
                # https://www.jaegertracing.io/docs/2.3/apis/#opentelemetry-protocol
                #
                # The real question is what COS and COS-lite accept.
                #
                # json_url = 'http://localhost:9411/api/v2/spans'
                # TODO: session=<custom session that groks ca= better>
                # zipkin_exporter = ZipkinExporter(
                #    endpoint=json_url, timeout=EXPORTER_TIMEOUT
                # )
                # This is actually the max delay value in the sequence 1, 2, ..., MAX
                # Set to 1 to disable sending live data (buffered data is still eventually sent)
                # Set to 2 (or more) to enable sending live data (after buffered)
                #
                # _MAX_RETRY_TIMEOUT = 2 with timeout=1 means:
                # - 1 attempt to send live, 1s sleep in the worst case
                # _MAX_RETRY_TIMEOUT = 3 or 4 with timeout=1 means:
                # - 1st attempt, 1s sleep, 2nd attempt, 1s sleep in the worst case
                real_exporter._MAX_RETRY_TIMEOUT = 2  # pyright: ignore[reportAttributeAccessIssue]
            else:
                real_exporter = None

            _exporter.real_exporter = real_exporter
            _exporter.settings = (url, ca)

    _exporter.buffer.mark_observed()


def shutdown_tracing() -> None:
    """Shutdown tracing, which is expected to flush the buffered data out."""
    get_tracer_provider().shutdown()  # type: ignore
