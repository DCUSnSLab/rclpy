# Copyright 2026 nev
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
# writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

from __future__ import annotations

from contextlib import ExitStack
from enum import IntEnum
import inspect
import threading
import time
from typing import Callable, Dict, List, Optional, TYPE_CHECKING, Union

from rclpy.callback_groups import ReentrantCallbackGroup
from rclpy.exceptions import InvalidHandle
from rclpy.impl.implementation_singleton import rclpy_implementation as _rclpy
from rclpy.subscription import Subscription
from rclpy.task import Task

if TYPE_CHECKING:
    from rclpy.executors import TimeoutObject


class _ClaimedTask(Task):
    def __init__(self, callback, executor, token):
        super().__init__(callback, executor=executor)
        self._core = executor._core
        self._token = token

    def _release(self):
        if self._token is not None:
            self._core.complete(self._token)
            self._token = None

    def __call__(self):
        try:
            super().__call__()
        finally:
            if self.done() or self.cancelled():
                self._release()

    def cancel(self):
        super().cancel()
        self._release()


class _DirectCall:
    """Take and call a plain subscription callback without a Task or coroutine."""

    __slots__ = ('_executor', '_sub', '_token', '_exception')

    def __init__(self, executor, sub, token):
        self._executor = executor
        self._sub = sub
        self._token = token
        self._exception = None

    def __call__(self):
        executor, sub = self._executor, self._sub
        group = sub.callback_group
        try:
            if executor._is_shutdown or not group.beginning_execution(sub):
                return
            try:
                with executor._work_tracker:
                    try:
                        with sub.handle:
                            info = sub.handle.take_message(sub.msg_type, sub.raw)
                    except InvalidHandle:
                        return
                    if info is not None:
                        if sub._callback_type is Subscription.CallbackType.MessageOnly:
                            sub.callback(info[0])
                        else:
                            sub.callback(*info)
            finally:
                group.ending_execution(sub)
        except BaseException as error:
            self._exception = error
        finally:
            executor._core.complete(self._token)

    def done(self):
        return True

    def cancelled(self):
        return False

    def exception(self):
        return self._exception

    def result(self):
        if self._exception is not None:
            raise self._exception


class SelectionPolicy(IntEnum):
    """Selection order for the shared scheduler."""

    FIFO = 0
    WAIT_SET = 1
    PRIORITY = 2


class RclExecutorMixin:
    """Execute Python tasks using the shared rcl event scheduler."""

    def _init_core(
        self, *, selection_policy: Optional[SelectionPolicy] = None,
        partitioned: bool = False, on_thread_start: Optional[Callable] = None
    ):
        if selection_policy is None:
            selection_policy = SelectionPolicy.WAIT_SET if partitioned else SelectionPolicy.FIFO
        self._partitioned = partitioned
        self._on_thread_start = on_thread_start
        self._partition_running = False
        self._partition_stopping = False
        self._groups = {}
        self._resuming = {}
        self._core = _rclpy.RclExecutorCore(int(SelectionPolicy(selection_policy)))
        self._core.set_partitioned(partitioned)
        self._entities: Dict = {}
        self._dirty = True
        self._selected_nodes = ()
        self._plain: Dict = {}
        self._claimed_item = None
        self._core_thread_pool = False
        self._internal = [self._core.add(5, guard.handle, 0, 0)
                          for guard in [self._guard, self._sigint_gc]]

    def _core_add_node(self, node):
        """Associate a node, refusing Reentrant groups in partition mode."""
        if self._partitioned:
            entities = [*node.subscriptions, *node.timers, *node.services, *node.clients,
                        *node.guards, *node.waitables]
            if any(isinstance(e.callback_group, ReentrantCallbackGroup) for e in entities):
                raise ValueError('partition mode refuses Reentrant callback groups')
        from rclpy.executors import Executor
        return Executor.add_node(self, node)

    def _stop_partitions(self):
        self._partition_stopping = True
        self._core.stop()
        for event in list(self._resuming.values()):
            event.set()

    def _thread_started(self, index, group):
        try:
            if self._on_thread_start is not None:
                self._on_thread_start(index, self._groups.get(group))
        except BaseException:
            self._stop_partitions()
            raise

    def _partition_sync(self):
        self._collect(self.get_nodes())
        with self._tasks_lock:
            ready = list(self._ready_tasks)
            self._ready_tasks.clear()
            for task in list(self._pending_tasks):
                if task.done() or task.cancelled():
                    del self._pending_tasks[task]
        for task in ready:
            if not task.done() and not task.cancelled():
                self._core.post(task)

    def _partition_execute(self, item):
        try:
            self._partition_dispatch(item)
        except BaseException:
            self._stop_partitions()
            raise

    def _partition_dispatch(self, item):
        if self._is_shutdown or not self._context.ok():
            self._stop_partitions()
            return
        key, _, kind = item
        if kind == 6:
            task = key if isinstance(key, Task) else Task(key, executor=self)
            task()
            if task.done():
                task.result()
            return
        elif key == 0:
            self._partition_sync()
            return
        elif key in self._entities:
            entity, node, waitset, _, _ = self._entities[key]
            task = self._claimed_handler(entity, node, waitset, None, kind)
        else:
            return
        event = threading.Event()
        self._resuming[task] = event
        try:
            while not self._is_shutdown and not self._partition_stopping:
                event.clear()
                task()
                if task.done() or task.cancelled():
                    task.result()
                    return
                event.wait()
            task.cancel()
        finally:
            del self._resuming[task]

    def _core_remove_node(self, node) -> None:
        """Remove a node and detach its readiness callbacks before it can move."""
        from rclpy.executors import Executor
        Executor._stock_remove_node(self, node)
        for key, entry in list(self._entities.items()):
            if entry[1] is node:
                self._remove_entity(key)
        self._dirty = True

    def _core_wake(self) -> None:
        """Recollect entities and interrupt the current wait."""
        self._dirty = True
        self._core.wake()
        if self._partition_running:
            self._core.post(self._partition_sync)

    def _core_call_task_in_next_spin(self, task: Task) -> None:
        event = self._resuming.get(task)
        if event is not None:
            event.set()
            return
        if self._partition_running:
            self._core.post(task)
            return
        with self._tasks_lock:
            self._ready_tasks.append(task)
        self._core.wake()

    def _remove_entity(self, key):
        _, _, _, registrations, stack = self._entities.pop(key)
        self._plain.pop(key, None)
        for registration in registrations:
            self._core.remove(registration)
        stack.close()

    def _collect(self, nodes):
        selected = tuple(nodes)
        if not self._dirty and selected == self._selected_nodes:
            return
        self._dirty = False
        self._selected_nodes = selected
        desired = {}
        for node in nodes:
            for kind, entities in enumerate([node.subscriptions, node.timers, node.services,
                                             node.clients, [], node.guards, node.waitables]):
                for entity in entities:
                    desired[id(entity)] = (entity, node, kind)
        for key in list(self._entities):
            if key not in desired:
                self._remove_entity(key)
        groups = {id(entity.callback_group): entity.callback_group
                  for entity, _, _ in desired.values()}
        for key in self._groups.keys() - groups.keys():
            self._core.remove_group(key)
        self._groups = groups
        for key, (entity, node, kind) in desired.items():
            if key in self._entities:
                continue
            group = entity.callback_group
            self._core.add_group(id(group), isinstance(group, ReentrantCallbackGroup))
            stack = ExitStack()
            if kind == 6:
                count = entity.get_num_entities()
                with self._context.handle:
                    waitset = _rclpy.WaitSet(
                        count.num_subscriptions, count.num_guard_conditions, count.num_timers,
                        count.num_clients, count.num_services, count.num_events,
                        self._context.handle)
                stack.enter_context(waitset)
                waitset.clear_entities()
                entity.add_to_wait_set(waitset)
                registrations = self._core.add_waitable(entity, waitset, key, id(group))
            else:
                waitset = None
                registrations = [self._core.add(kind, entity.handle, key, id(group))]
            self._entities[key] = (entity, node, waitset, registrations, stack)
            if kind == 0:
                self._is_plain(entity)

    def _is_plain(self, sub):
        callback = sub.callback
        cached = self._plain.get(id(sub))
        if cached is None or cached[0] is not callback:
            cached = (callback, not inspect.iscoroutinefunction(callback))
            self._plain[id(sub)] = cached
            if cached[1]:
                self._core.set_plain(self._entities[id(sub)][3][0], sub, callback)
        return cached[1]

    def _claimed_handler(self, entity, node, waitset, token, kind):
        from rclpy.executors import await_or_execute, TaskData
        entity._executor_event = True

        async def handler():
            if self._is_shutdown or not entity.callback_group.beginning_execution(entity):
                entity._executor_event = False
                return
            try:
                with self._work_tracker:
                    if waitset is not None:
                        if not entity.is_ready(waitset):
                            return
                        execute = self._take_waitable(entity)
                    elif kind == 1:
                        async def execute():
                            await await_or_execute(entity.callback)
                    else:
                        take = {0: self._take_subscription, 2: self._take_service,
                                3: self._take_client, 5: self._take_guard_condition}[kind]
                        execute = take(entity)
                    entity._executor_event = False
                    if execute is not None:
                        await execute()
            finally:
                entity._executor_event = False
                entity.callback_group.ending_execution(entity)

        task = _ClaimedTask(handler(), self, token)
        with self._tasks_lock:
            self._pending_tasks[task] = TaskData(source_entity=entity, source_node=node)
        return task

    def _core_wait_for_ready_callbacks(
        self, timeout_sec: Optional[Union[float, TimeoutObject]] = None,
        nodes: Optional[List] = None, condition: Callable[[], bool] = lambda: False,
        in_pass: bool = False,
    ):
        from rclpy.executors import (
            ConditionReachedException, ExternalShutdownException, ShutdownException,
            TimeoutException, TimeoutObject)
        timeout = timeout_sec.timeout if isinstance(timeout_sec, TimeoutObject) else timeout_sec
        end = None if timeout is None or timeout < 0 else time.monotonic() + timeout
        while True:
            if self._is_shutdown:
                raise ShutdownException()
            if not self._context.ok():
                raise ExternalShutdownException()
            if condition():
                raise ConditionReachedException()
            selected = self.get_nodes() if nodes is None else nodes
            self._collect(selected)
            yielded_task = False
            with self._tasks_lock:
                for task in list(self._pending_tasks):
                    if task.done() or task.cancelled():
                        del self._pending_tasks[task]
                ready_count = len(self._ready_tasks)
            for _ in range(ready_count):
                with self._tasks_lock:
                    task = self._ready_tasks.popleft()
                    data = self._pending_tasks.get(task)
                if data is None or task.done() or task.cancelled():
                    continue
                if data.source_node is not None and data.source_node not in selected:
                    with self._tasks_lock:
                        self._ready_tasks.append(task)
                    continue
                yielded_task = True
                yield task, data.source_entity, data.source_node
                return
            item, self._claimed_item = self._claimed_item, None
            if item is None:
                item = self._core.next_in_pass() if in_pass else self._core.next()
            if item is not None:
                key, token, kind = item
                if kind == 6:
                    self._core.complete(token)
                    task = key if isinstance(key, Task) else Task(key, executor=self)
                    yield task, None, None
                    return
                if key == 0 or key not in self._entities:
                    self._core.complete(token)
                    self._dirty = True
                    continue
                entity, node, waitset, _, _ = self._entities[key]
                if (kind == 0 and waitset is None and not self._core_thread_pool and
                        self._is_plain(entity)):
                    yield _DirectCall(self, entity, token), entity, node
                    return
                task = self._claimed_handler(entity, node, waitset, token, kind)
                yield task, entity, node
                return
            if yielded_task:
                return
            remaining = -1 if end is None else max(0, int((end - time.monotonic()) * 1e9))
            if remaining == 0:
                raise TimeoutException()
            self._core.wait(remaining)

    def _core_spin_some(self, end: Optional[float]) -> None:
        from rclpy.executors import TimeoutException
        self._core.begin_pass()
        while end is None or time.monotonic() < end:
            try:
                handler, _, _ = next(
                    self._core_wait_for_ready_callbacks(0, None, lambda: False, True))
            except (StopIteration, TimeoutException):
                return
            handler()
            if handler.exception() is not None:
                raise handler.exception()

    def _core_dispatch(self, batch: int, wait_condition: Callable[[], bool]) -> bool:
        """Run up to ``batch`` plain subscription callbacks without building Tasks."""
        if self._claimed_item is not None or self._is_shutdown or self._dirty or \
                self._ready_tasks or wait_condition():
            return False
        with self._work_tracker:
            count, self._claimed_item = self._core.dispatch(batch)
        return count > 0

    def _core_spin(self) -> None:
        self._enter_spin()
        try:
            if self._partitioned:
                self._partition_sync()
                self._partition_running = True
                try:
                    self._core.spin(self._partition_execute, self._thread_started)
                finally:
                    self._partition_running = False
                return
            self._thread_started(0, 0)
            while self._context.ok() and not self._is_shutdown:
                if not self._core_dispatch(64, lambda: False):
                    self._spin_once_impl()
        finally:
            self._exit_spin()

    def _core_shutdown(self, timeout_sec: Optional[float] = None) -> bool:
        """Stop dispatch and cancel suspended tasks, releasing their groups."""
        self._is_shutdown = True
        self._core.stop()
        for event in list(self._resuming.values()):
            event.set()
        with self._tasks_lock:
            tasks = list(self._pending_tasks)
        for task in tasks:
            if not task.executing():
                task.cancel()
        for key in list(self._entities):
            self._remove_entity(key)
        for registration in self._internal:
            self._core.remove(registration)
        self._internal.clear()
        from rclpy.executors import Executor
        return Executor._stock_shutdown(self, timeout_sec)
