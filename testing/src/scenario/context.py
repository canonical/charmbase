#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

from __future__ import annotations

import functools
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import (
    Generic,
    TYPE_CHECKING,
    Any,
    Callable,
    Mapping,
    cast,
)

import ops
from ops._private.harness import ActionFailed

from .errors import (
    AlreadyEmittedError,
    ContextSetupError,
    MetadataNotFoundError,
)
from .logger import logger as scenario_logger
from .runtime import Runtime
from .state import (
    CharmType,
    CheckInfo,
    Container,
    Notice,
    Secret,
    Storage,
    _Action,
    _CharmSpec,
    _Event,
)

if TYPE_CHECKING:  # pragma: no cover
    from ops._private.harness import ExecArgs
    from .ops_main_mock import Ops
    from .state import (
        AnyJson,
        JujuLogLine,
        RelationBase,
        State,
        _EntityStatus,
    )

logger = scenario_logger.getChild("runtime")

_DEFAULT_JUJU_VERSION = "3.5"


class Manager(Generic[CharmType]):
    """Context manager to offer test code some runtime charm object introspection.

    This class should not be instantiated directly: use a :class:`Context`
    in a ``with`` statement instead, for example::

        ctx = Context(MyCharm)
        with ctx(ctx.on.start(), State()) as manager:
            manager.charm.setup()
            manager.run()
    """

    def __init__(
        self,
        ctx: Context[CharmType],
        arg: _Event,
        state_in: State,
    ):
        self._ctx = ctx
        self._arg = arg
        self._state_in = state_in

        self._emitted: bool = False

        self.ops: Ops | None = None

    @property
    def charm(self) -> CharmType:
        """The charm object instantiated by ops to handle the event.

        The charm is only available during the context manager scope.
        """
        if not self.ops:
            raise RuntimeError(
                "you should __enter__ this context manager before accessing this",
            )
        return cast(CharmType, self.ops.charm)

    @property
    def _runner(self):
        return self._ctx._run  # noqa

    def __enter__(self):
        self._wrapped_ctx = wrapped_ctx = self._runner(self._arg, self._state_in)
        ops = wrapped_ctx.__enter__()
        self.ops = ops
        return self

    def run(self) -> State:
        """Emit the event and proceed with charm execution.

        This can only be done once.
        """
        if self._emitted:
            raise AlreadyEmittedError("Can only run once.")
        self._emitted = True

        # wrap up Runtime.exec() so that we can gather the output state
        self._wrapped_ctx.__exit__(None, None, None)

        assert self._ctx._output_state is not None
        return self._ctx._output_state

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any):  # noqa: U100
        if not self._emitted:
            logger.debug(
                "user didn't emit the event within the context manager scope. Doing so implicitly upon exit...",
            )
            self.run()


def _copy_doc(original_func: Callable[..., Any]):
    """Copy the docstring from `original_func` to the wrapped function."""

    def decorator(wrapper_func: Callable[..., Any]):
        @functools.wraps(wrapper_func)
        def wrapped(*args: Any, **kwargs: Any):
            return wrapper_func(*args, **kwargs)

        wrapped.__doc__ = original_func.__doc__
        return wrapped

    return decorator


class CharmEvents:
    """Events generated by Juju or ops pertaining to the application lifecycle.

    The events listed as attributes of this class should be accessed via the
    :attr:`Context.on` attribute. For example::

        ctx.run(ctx.on.config_changed(), state)

    This behaves similarly to the :class:`ops.CharmEvents` class but is much
    simpler as there are no dynamically named attributes, and no ``__getattr__``
    version to get events. In addition, all of the attributes are methods,
    which are used to connect the event to the specific object that they relate
    to (or, for simpler events like "start" or "stop", take no arguments).
    """

    @staticmethod
    @_copy_doc(ops.InstallEvent)
    def install():
        return _Event("install")

    @staticmethod
    @_copy_doc(ops.StartEvent)
    def start():
        return _Event("start")

    @staticmethod
    @_copy_doc(ops.StopEvent)
    def stop():
        return _Event("stop")

    @staticmethod
    @_copy_doc(ops.RemoveEvent)
    def remove():
        return _Event("remove")

    @staticmethod
    @_copy_doc(ops.UpdateStatusEvent)
    def update_status():
        return _Event("update_status")

    @staticmethod
    @_copy_doc(ops.ConfigChangedEvent)
    def config_changed():
        return _Event("config_changed")

    @staticmethod
    @_copy_doc(ops.UpgradeCharmEvent)
    def upgrade_charm():
        return _Event("upgrade_charm")

    @staticmethod
    @_copy_doc(ops.PreSeriesUpgradeEvent)
    def pre_series_upgrade():
        return _Event("pre_series_upgrade")

    @staticmethod
    @_copy_doc(ops.PostSeriesUpgradeEvent)
    def post_series_upgrade():
        return _Event("post_series_upgrade")

    @staticmethod
    @_copy_doc(ops.LeaderElectedEvent)
    def leader_elected():
        return _Event("leader_elected")

    @staticmethod
    @_copy_doc(ops.SecretChangedEvent)
    def secret_changed(secret: Secret):
        if secret.owner:
            raise ValueError(
                "This unit will never receive secret-changed for a secret it owns.",
            )
        return _Event("secret_changed", secret=secret)

    @staticmethod
    @_copy_doc(ops.SecretExpiredEvent)
    def secret_expired(secret: Secret, *, revision: int):
        if not secret.owner:
            raise ValueError(
                "This unit will never receive secret-expire for a secret it does not own.",
            )
        return _Event("secret_expired", secret=secret, secret_revision=revision)

    @staticmethod
    @_copy_doc(ops.SecretRotateEvent)
    def secret_rotate(secret: Secret):
        if not secret.owner:
            raise ValueError(
                "This unit will never receive secret-rotate for a secret it does not own.",
            )
        return _Event("secret_rotate", secret=secret)

    @staticmethod
    @_copy_doc(ops.SecretRemoveEvent)
    def secret_remove(secret: Secret, *, revision: int):
        if not secret.owner:
            raise ValueError(
                "This unit will never receive secret-removed for a secret it does not own.",
            )
        return _Event("secret_remove", secret=secret, secret_revision=revision)

    @staticmethod
    def collect_app_status():
        """Event triggered at the end of every hook to collect app statuses for evaluation"""
        return _Event("collect_app_status")

    @staticmethod
    def collect_unit_status():
        """Event triggered at the end of every hook to collect unit statuses for evaluation"""
        return _Event("collect_unit_status")

    @staticmethod
    @_copy_doc(ops.RelationCreatedEvent)
    def relation_created(relation: RelationBase):
        return _Event(f"{relation.endpoint}_relation_created", relation=relation)

    @staticmethod
    @_copy_doc(ops.RelationJoinedEvent)
    def relation_joined(relation: RelationBase, *, remote_unit: int | None = None):
        return _Event(
            f"{relation.endpoint}_relation_joined",
            relation=relation,
            relation_remote_unit_id=remote_unit,
        )

    @staticmethod
    @_copy_doc(ops.RelationChangedEvent)
    def relation_changed(
        relation: RelationBase,
        *,
        remote_unit: int | None = None,
    ):
        return _Event(
            f"{relation.endpoint}_relation_changed",
            relation=relation,
            relation_remote_unit_id=remote_unit,
        )

    @staticmethod
    @_copy_doc(ops.RelationDepartedEvent)
    def relation_departed(
        relation: RelationBase,
        *,
        remote_unit: int | None = None,
        departing_unit: int | None = None,
    ):
        return _Event(
            f"{relation.endpoint}_relation_departed",
            relation=relation,
            relation_remote_unit_id=remote_unit,
            relation_departed_unit_id=departing_unit,
        )

    @staticmethod
    @_copy_doc(ops.RelationBrokenEvent)
    def relation_broken(relation: RelationBase):
        return _Event(f"{relation.endpoint}_relation_broken", relation=relation)

    @staticmethod
    @_copy_doc(ops.StorageAttachedEvent)
    def storage_attached(storage: Storage):
        return _Event(f"{storage.name}_storage_attached", storage=storage)

    @staticmethod
    @_copy_doc(ops.StorageDetachingEvent)
    def storage_detaching(storage: Storage):
        return _Event(f"{storage.name}_storage_detaching", storage=storage)

    @staticmethod
    @_copy_doc(ops.PebbleReadyEvent)
    def pebble_ready(container: Container):
        return _Event(f"{container.name}_pebble_ready", container=container)

    @staticmethod
    @_copy_doc(ops.PebbleCustomNoticeEvent)
    def pebble_custom_notice(container: Container, notice: Notice):
        return _Event(
            f"{container.name}_pebble_custom_notice",
            container=container,
            notice=notice,
        )

    @staticmethod
    @_copy_doc(ops.PebbleCheckFailedEvent)
    def pebble_check_failed(container: Container, info: CheckInfo):
        return _Event(
            f"{container.name}_pebble_check_failed",
            container=container,
            check_info=info,
        )

    @staticmethod
    @_copy_doc(ops.PebbleCheckRecoveredEvent)
    def pebble_check_recovered(container: Container, info: CheckInfo):
        return _Event(
            f"{container.name}_pebble_check_recovered",
            container=container,
            check_info=info,
        )

    @staticmethod
    @_copy_doc(ops.ActionEvent)
    def action(
        name: str,
        params: Mapping[str, AnyJson] | None = None,
        id: str | None = None,
    ):
        kwargs: dict[str, Any] = {}
        if params:
            kwargs["params"] = params
        if id:
            kwargs["id"] = id
        return _Event(f"{name}_action", action=_Action(name, **kwargs))


class Context(Generic[CharmType]):
    """Represents a simulated charm's execution context.

    The main entry point to running a test. It contains:

    - the charm source code being executed
    - the metadata files associated with it
    - a charm project repository root
    - the Juju version to be simulated

    After you have instantiated ``Context``, typically you will call :meth:`run()` to execute the
    charm once, write any assertions you like on the output state returned by the call, write any
    assertions you like on the ``Context`` attributes, then discard the ``Context``.

    Each ``Context`` instance is in principle designed to be single-use:
    ``Context`` is not cleaned up automatically between charm runs.

    Any side effects generated by executing the charm, that are not rightful part of the
    ``State``, are in fact stored in the ``Context``:

    - :attr:`juju_log`
    - :attr:`app_status_history`
    - :attr:`unit_status_history`
    - :attr:`workload_version_history`
    - :attr:`removed_secret_revisions`
    - :attr:`requested_storages`
    - :attr:`emitted_events`
    - :attr:`action_logs`
    - :attr:`action_results`

    This allows you to write assertions not only on the output state, but also, to some
    extent, on the path the charm took to get there.

    A typical test will look like::

        from charm import MyCharm, MyCustomEvent  # noqa

        def test_foo():
            # Arrange: set the context up
            ctx = Context(MyCharm)
            # Act: prepare the state and emit an event
            state_out = ctx.run(ctx.on.update_status(), State())
            # Assert: verify the output state is what you think it should be
            assert state_out.unit_status == ActiveStatus('foobar')
            # Assert: verify the Context contains what you think it should
            assert len(c.emitted_events) == 4
            assert isinstance(c.emitted_events[3], MyCustomEvent)

    If you need access to the charm object that will handle the event, use the
    class in a ``with`` statement, like::

        def test_foo():
            ctx = Context(MyCharm)
            with ctx(ctx.on.start(), State()) as manager:
                manager.charm._some_private_setup()
                manager.run()
    """

    juju_log: list[JujuLogLine]
    """A record of what the charm has sent to juju-log"""
    app_status_history: list[_EntityStatus]
    """A record of the app statuses the charm has set"""
    unit_status_history: list[_EntityStatus]
    """A record of the unit statuses the charm has set"""
    workload_version_history: list[str]
    """A record of the workload versions the charm has set"""
    removed_secret_revisions: list[int]
    """A record of the secret revisions the charm has removed"""
    emitted_events: list[ops.EventBase]
    """A record of the events (including custom) that the charm has processed"""
    requested_storages: dict[str, int]
    """A record of the storages the charm has requested"""
    action_logs: list[str]
    """The logs associated with the action output, set by the charm with :meth:`ops.ActionEvent.log`

    This will be empty when handling a non-action event.
    """
    action_results: dict[str, Any] | None
    """A key-value mapping assigned by the charm as a result of the action.

    This will be ``None`` if the charm never calls :meth:`ops.ActionEvent.set_results`
    """
    on: CharmEvents
    """The events that this charm can respond to.

    Use this when calling :meth:`run` to specify the event to emit.
    """

    def __init__(
        self,
        charm_type: type[CharmType],
        meta: dict[str, Any] | None = None,
        *,
        actions: dict[str, Any] | None = None,
        config: dict[str, Any] | None = None,
        charm_root: str | Path | None = None,
        juju_version: str = _DEFAULT_JUJU_VERSION,
        capture_deferred_events: bool = False,
        capture_framework_events: bool = False,
        app_name: str | None = None,
        unit_id: int | None = 0,
        app_trusted: bool = False,
    ):
        """Represents a simulated charm's execution context.

        If the charm, say, expects a ``./src/foo/bar.yaml`` file present relative to the
        execution cwd, you need to use the ``charm_root`` argument. For example::

            import tempfile
            virtual_root = tempfile.TemporaryDirectory()
            local_path = Path(local_path.name)
            (local_path / 'foo').mkdir()
            (local_path / 'foo' / 'bar.yaml').write_text('foo: bar')
            Context(... charm_root=virtual_root).run(...)

        :arg charm_type: the :class:`ops.CharmBase` subclass to handle the event.
        :arg meta: charm metadata to use. Needs to be a valid metadata.yaml format (as a dict).
            If none is provided, we will search for a ``metadata.yaml`` file in the charm root.
        :arg actions: charm actions to use. Needs to be a valid actions.yaml format (as a dict).
            If none is provided, we will search for a ``actions.yaml`` file in the charm root.
        :arg config: charm config to use. Needs to be a valid config.yaml format (as a dict).
            If none is provided, we will search for a ``config.yaml`` file in the charm root.
        :arg juju_version: Juju agent version to simulate.
        :arg app_name: App name that this charm is deployed as. Defaults to the charm name as
            defined in the metadata.
        :arg unit_id: Unit ID that this charm is deployed as.
        :arg app_trusted: whether the charm has Juju trust (deployed with ``--trust`` or added with
            ``juju trust``).
        :arg charm_root: virtual charm filesystem root the charm will be executed with.
        """

        if not any((meta, actions, config)):
            logger.debug("Autoloading charmspec...")
            try:
                spec: _CharmSpec[CharmType] = _CharmSpec.autoload(charm_type)
            except MetadataNotFoundError as e:
                raise ContextSetupError(
                    f"Cannot setup scenario with `charm_type`={charm_type}. "
                    f"Did you forget to pass `meta` to this Context?",
                ) from e

        else:
            if not meta:
                meta = {"name": str(charm_type.__name__)}
            spec = _CharmSpec(
                charm_type=charm_type,
                meta=meta,
                actions=actions,
                config=config,
            )

        self.charm_spec = spec
        self.charm_root = charm_root
        self.juju_version = juju_version
        if juju_version.split(".")[0] == "2":
            logger.warning(
                "Juju 2.x is closed and unsupported. You may encounter inconsistencies.",
            )

        self._app_name = app_name
        self._unit_id = unit_id
        self.app_trusted = app_trusted
        self._tmp = tempfile.TemporaryDirectory()

        # config for what events to be captured in emitted_events.
        self.capture_deferred_events = capture_deferred_events
        self.capture_framework_events = capture_framework_events

        # streaming side effects from running an event
        self.juju_log: list[JujuLogLine] = []
        self.app_status_history: list[_EntityStatus] = []
        self.unit_status_history: list[_EntityStatus] = []
        self.exec_history: dict[str, list[ExecArgs]] = {}
        self.workload_version_history: list[str] = []
        self.removed_secret_revisions: list[int] = []
        self.emitted_events: list[ops.EventBase] = []
        self.requested_storages: dict[str, int] = {}

        # set by Runtime.exec() in self._run()
        self._output_state: State | None = None

        # operations (and embedded tasks) from running actions
        self.action_logs: list[str] = []
        self.action_results: dict[str, Any] | None = None
        self._action_failure_message: str | None = None

        self.on = CharmEvents()

    def _set_output_state(self, output_state: State):
        """Hook for Runtime to set the output state."""
        self._output_state = output_state

    def _get_container_root(self, container_name: str):
        """Get the path to a tempdir where this container's simulated root will live."""
        return Path(self._tmp.name) / "containers" / container_name

    def _get_storage_root(self, name: str, index: int) -> Path:
        """Get the path to a tempdir where this storage's simulated root will live."""
        storage_root = Path(self._tmp.name) / "storages" / f"{name}-{index}"
        # in the case of _get_container_root, _MockPebbleClient will ensure the dir exists.
        storage_root.mkdir(parents=True, exist_ok=True)
        return storage_root

    def _record_status(self, state: State, is_app: bool):
        """Record the previous status before a status change."""
        if is_app:
            self.app_status_history.append(state.app_status)
        else:
            self.unit_status_history.append(state.unit_status)

    def __call__(self, event: _Event, state: State):
        """Context manager to introspect live charm object before and after the event is emitted.

        Usage::

            ctx = Context(MyCharm)
            with ctx(ctx.on.start(), State()) as manager:
                manager.charm._some_private_setup()
                manager.run()  # this will fire the event
                assert manager.charm._some_private_attribute == "bar"  # noqa

        Args:
            event: the event that the charm will respond to.
            state: the :class:`State` instance to use when handling the event.
        """
        return Manager(self, event, state)

    def run_action(self, action: str, state: State):
        """Use `run()` instead.

        :private:
        """
        raise AttributeError(
            f"call with `ctx.run`, like `ctx.run(ctx.on.action({action!r})` "
            "and find the results in `ctx.action_results`",
        )

    def run(self, event: _Event, state: State) -> State:
        """Trigger a charm execution with an event and a State.

        Calling this function will call ``ops.main`` and set up the context according to the
        specified :class:`State`, then emit the event on the charm.

        :arg event: the event that the charm will respond to. Use the :attr:`on` attribute to
            specify the event; for example: ``ctx.on.start()``.
        :arg state: the :class:`State` instance to use as data source for the hook tool calls that
            the charm will invoke when handling the event.
        """
        # Help people transition from Scenario 6:
        if isinstance(event, str):
            event = event.replace("-", "_")  # type: ignore
            if event in (
                "install",
                "start",
                "stop",
                "remove",
                "update_status",
                "config_changed",
                "upgrade_charm",
                "pre_series_upgrade",
                "post_series_upgrade",
                "leader_elected",
                "collect_app_status",
                "collect_unit_status",
            ):  # type: ignore
                suggested = f"{event}()"
            elif event in ("secret_changed", "secret_rotate"):  # type: ignore
                suggested = f"{event}(my_secret)"
            elif event in ("secret_expired", "secret_remove"):  # type: ignore
                suggested = f"{event}(my_secret, revision=1)"
            elif event in (
                "relation_created",
                "relation_joined",
                "relation_changed",
                "relation_departed",
                "relation_broken",
            ):  # type: ignore
                suggested = f"{event}(my_relation)"
            elif event in ("storage_attached", "storage_detaching"):  # type: ignore
                suggested = f"{event}(my_storage)"
            elif event == "pebble_ready":
                suggested = f"{event}(my_container)"
            elif event == "pebble_custom_notice":
                suggested = f"{event}(my_container, my_notice)"
            else:
                suggested = "event()"
            raise TypeError(
                f"call with an event from `ctx.on`, like `ctx.on.{suggested}`",
            )
        if callable(event):
            raise TypeError(
                "You should call the event method. Did you forget to add parentheses?",
            )

        if event.action:
            # Reset the logs, failure status, and results, in case the context
            # is reused.
            self.action_logs.clear()
            if self.action_results is not None:
                self.action_results.clear()
            self._action_failure_message = None
        with self._run(event=event, state=state) as ops:
            ops.emit()
        # We know that the output state will have been set by this point,
        # so let the type checkers know that too.
        assert self._output_state is not None
        if event.action:
            if self._action_failure_message is not None:
                raise ActionFailed(
                    self._action_failure_message,
                    state=self._output_state,  # type: ignore
                )
        return self._output_state

    @contextmanager
    def _run(self, event: _Event, state: State):
        runtime = Runtime(
            charm_spec=self.charm_spec,
            juju_version=self.juju_version,
            charm_root=self.charm_root,
            app_name=self._app_name,
            unit_id=self._unit_id,
        )
        with runtime.exec(
            state=state,
            event=event,
            context=self,  # type: ignore
        ) as ops:
            yield ops
