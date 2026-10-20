# Copyright 2026 nev
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
# writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

from typing import Callable, Optional, Union

from rclpy._rcl_executor import RclExecutorMixin, SelectionPolicy
from rclpy.callback_groups import ReentrantCallbackGroup
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
