# Copyright 2024 Canonical Ltd.
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

"""Benchmark tests for ops-scenario."""

import dataclasses
import pathlib
import sys

import ops
from ops import testing

sys.path.append(
    str(
        pathlib.Path(__file__).parent.parent.parent.parent
        / "test"
        / "charms"
        / "test_benchmark"
        / "src"
    )
)

from benchmark_charm import BenchmarkCharm


# Note: the 'benchmark' argument here is a fixture that pytest-benchmark
# automatically makes available to all tests.
def test_context_explicit_meta(benchmark):
    ctx = benchmark(testing.Context, ops.CharmBase, meta={"name": "foo"})
    assert isinstance(ctx, testing.Context)


def test_run_no_observer(benchmark):
    ctx = testing.Context(BenchmarkCharm)
    benchmark(ctx.run, ctx.on.start(), testing.State())
    assert len({e.handle.kind for e in ctx.emitted_events}) == 1


def test_run_observed(benchmark):
    ctx = testing.Context(BenchmarkCharm)
    benchmark(ctx.run, ctx.on.stop(), testing.State())
    assert len({e.handle.kind for e in ctx.emitted_events}) == 1


def test_context_explicit_meta_config_actions(benchmark):
    ctx = benchmark(
        testing.Context,
        ops.CharmBase,
        meta={"name": "foo"},
        actions={"act": {"description": "foo"}},
        config={"options": {"conf": {"type": "int", "description": "bar"}}},
    )
    ctx.run(ctx.on.action("act"), testing.State(config={"conf": 10}))
    assert len({e.handle.kind for e in ctx.emitted_events}) == 1


def test_context_autoload_meta(benchmark):
    ctx = benchmark(testing.Context, BenchmarkCharm)
    assert isinstance(ctx, testing.Context)


def test_many_tests_explicit_meta(benchmark):
    def mock_pytest():
        """Simulate running multiple tests against the same charm."""
        for event in ("install", "start", "stop", "remove"):
            for _ in range(5):
                ctx = testing.Context(ops.CharmBase, meta={"name": "foo"})
                ctx.run(getattr(ctx.on, event)(), testing.State())
                assert len({e.handle.kind for e in ctx.emitted_events}) == 1

    benchmark(mock_pytest)


def test_many_tests_autoload_meta(benchmark):
    def mock_pytest():
        """Simulate running multiple tests against the same charm."""
        for event in ("install", "start", "stop", "remove"):
            for _ in range(5):
                ctx = testing.Context(BenchmarkCharm)
                ctx.run(getattr(ctx.on, event)(), testing.State())
                assert len({e.handle.kind for e in ctx.emitted_events}) == 1

    benchmark(mock_pytest)


def test_lots_of_logs(benchmark):
    ctx = testing.Context(BenchmarkCharm)
    benchmark(ctx.run, ctx.on.update_status(), testing.State())
    assert len(ctx.juju_log) > 200


def test_full_state(benchmark):
    def fill_state():
        rel = testing.Relation("rel")
        peer = testing.PeerRelation("peer")
        network = testing.Network("MySpace")
        container = testing.Container("foo")
        storage = testing.Storage("bar")
        tcp = testing.TCPPort(22)
        icmp = testing.ICMPPort()
        udp = testing.UDPPort(8000)
        secret = testing.Secret({"password": "admin"})
        resource = testing.Resource(name="baz", path=".")
        stored_state = testing.StoredState(owner_path="BenchMarkCharm")
        state = testing.State(
            relations={rel, peer},
            networks={network},
            containers={container},
            storages={storage},
            opened_ports={tcp, icmp, udp},
            secrets={secret},
            resources={resource},
            stored_states={stored_state},
            app_status=testing.ActiveStatus(),
            unit_status=testing.BlockedStatus("I'm stuck!"),
        )
        return state

    ctx = testing.Context(BenchmarkCharm)
    state_in = benchmark(fill_state)
    state_out = ctx.run(ctx.on.start(), state_in)
    # stored_states is complicated: it will contain a stored state the
    # framework itself added (counting the number of events), so the
    # input and output state doesn't naively match. We strip that out and
    # compare it separately.
    state_in_dict = dataclasses.asdict(state_in)
    state_out_dict = dataclasses.asdict(state_out)
    # An owner_path of None means that it's owned by the framework.
    assert state_in_dict["stored_states"] == {
        ss for ss in state_out_dict["stored_states"] if ss.owner_path is not None
    }
    del state_in_dict["stored_states"]
    del state_out_dict["stored_states"]
    assert state_in_dict == state_out_dict


def test_deferred_events(benchmark):
    ctx = testing.Context(BenchmarkCharm, capture_deferred_events=True)
    deferred = ctx.on.stop().deferred(BenchmarkCharm._on_stop)
    state_in = testing.State(deferred=[deferred])
    state_out = benchmark(ctx.run, ctx.on.config_changed(), state_in)
    assert len(state_out.deferred) == 1
    assert len({e.handle.kind for e in ctx.emitted_events}) == 2
