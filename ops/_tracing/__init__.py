# Copyright 2025 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""The tracing facility of the Operator Framework.

TODO: quick start, usage example.
"""

from __future__ import annotations

import logging

from ops._tracing import hacks

# FIXME must this hack be run before OTEL packages are imported?
hacks.remove_stale_otel_sdk_packages()


try:
    from . import export
except ImportError:
    export = None


def setup_tracing(charm_class_name: str) -> None:
    """Setup tracing for this "dispatch" of the charm code."""
    if not export:
        return
    export.setup_tracing(charm_class_name)


def set_tracing_destination(
    *,
    url: str | None,
    ca: str | None = None,
) -> None:
    """Configure the destination service for tracing data."""
    if not export:
        return
    export.set_tracing_destination(url=url, ca=ca)


def shutdown_tracing() -> None:
    """Send out as much as possible, if possible."""
    if not export:
        return
    try:
        export.shutdown_tracing()
    except Exception:
        logging.exception('failed to flush tracing')
