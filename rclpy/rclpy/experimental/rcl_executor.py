# Copyright 2026 nev
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
# writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

from typing import Callable, Optional, Union

from rclpy._rcl_executor import _Source, RclExecutorMixin, SelectionPolicy
from rclpy.callback_groups import CallbackGroup, ReentrantCallbackGroup
from rclpy.context import Context
from rclpy.executors import SingleThreadedExecutor, TaskData, TimeoutObject
from rclpy.task import Task

__all__ = ['RclExecutor', 'SelectionPolicy']


class RclExecutor(SingleThreadedExecutor):
    """Execute Python tasks using the shared rcl event scheduler."""

    _use_core_default = False
    add_node = RclExecutorMixin._core_add_node

    def __init__(self, *, context: Optional[Context] = None,
                 selection_policy: Optional[SelectionPolicy] = None,
                 partitioned: bool = False, on_thread_start: Optional[Callable] = None):
        super().__init__(context=context)
        self._init_core(selection_policy=selection_policy, partitioned=partitioned,
                        on_thread_start=on_thread_start)

    def on_thread_start(self, callback):
        """Set the callback receiving a worker index and its group (or None)."""
        if self._partition_running:
            raise RuntimeError('executor is spinning')
        self._on_thread_start = callback

    def set_group_priority(self, group, priority: int) -> None:
        """Set a callback group's integer priority; larger values run first."""
        self._core.add_group(id(group), isinstance(group, ReentrantCallbackGroup))
        self._core.set_group_priority(id(group), priority)

    def add_source(self, callback_group: CallbackGroup, callback: Callable) -> int:
        """
        Register a callback that an event source outside of ROS schedules.

        The callback runs in the executor, once per notification, under the rules of
        ``callback_group`` like the callbacks of its entities: exclusion, priority and, in
        partition mode, the thread of the group. It takes no arguments and may be a coroutine
        function. Call :meth:`notify_source` from any thread when the source has work.

        :param callback_group: The callback group the callback runs in.
        :param callback: The callback.
        :return: The registration to pass to :meth:`notify_source` and :meth:`remove_source`.
        """
        reentrant = isinstance(callback_group, ReentrantCallbackGroup)
        if self._partitioned and reentrant:
            raise ValueError('partition mode refuses Reentrant callback groups')
        self._core.add_group(id(callback_group), reentrant)
        source = _Source(callback, callback_group)
        registration = self._core.add_source(source, id(callback_group))
        self._sources[registration] = source
        self._groups[id(callback_group)] = callback_group
        return registration

    def notify_source(self, source: int, count: int = 1) -> None:
        """Schedule ``count`` runs of a source's callback; a removed source is ignored."""
        self._core.notify(source, count)

    def remove_source(self, source: int) -> None:
        """Remove a source and the runs it has pending; a run already started completes."""
        self._core.remove(source)
        self._sources.pop(source, None)
        self._dirty = True

    def post(self, callback, *args, **kwargs) -> Task:
        """Post language work to the core and return its Task."""
        task = Task(callback, args, kwargs, executor=self)
        with self._tasks_lock:
            self._pending_tasks[task] = TaskData()
        self._core.post(task)
        return task

    def _spin_once_impl(
        self, timeout_sec: Optional[Union[float, TimeoutObject]] = None,
        wait_condition: Callable[[], bool] = lambda: False,
    ) -> None:
        if self._partitioned:
            raise RuntimeError('partition mode requires spin()')
        super()._spin_once_impl(timeout_sec, wait_condition)
