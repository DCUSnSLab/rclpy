# Copyright 2026 Open Source Robotics Foundation, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import signal
import subprocess
import sys
import time

import pytest
import rclpy
from rclpy.executors import ExternalShutdownException, SingleThreadedExecutor

SPINNER = """
import signal
import sys

import rclpy
from rclpy.executors import ExternalShutdownException, SingleThreadedExecutor

if sys.argv[1] == 'ignore':
    signal.signal(signal.SIGINT, signal.SIG_IGN)
rclpy.init()
node = rclpy.create_node('sigint_shutdown')
executor = SingleThreadedExecutor()
executor.add_node(node)
if sys.argv[2] != '0':
    node.create_timer(float(sys.argv[2]), lambda: None)
print('spinning', flush=True)
try:
    executor.spin()
except (KeyboardInterrupt, ExternalShutdownException):
    pass
print('shut down', flush=True)
"""


def interrupt_spin(sigint, timer_period):
    proc = subprocess.Popen(
        [sys.executable, '-c', SPINNER, sigint, str(timer_period)], stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True, cwd=os.path.dirname(__file__))
    try:
        assert proc.stdout.readline().strip() == 'spinning'
        time.sleep(1.0)
        proc.send_signal(signal.SIGINT)
        out, err = proc.communicate(timeout=10)
    finally:
        proc.kill()
    assert 'shut down' in out, err
    assert proc.returncode == 0, err


def test_sigint_ends_spin_when_python_ignores_sigint():
    # A process started in the background of a script inherits SIGINT ignored, so only the
    # rclpy handler sees it and spin must end through the context shutdown alone.
    interrupt_spin('ignore', 0)


def test_context_shut_down_before_the_wait_set_is_an_external_shutdown():
    # The signal handler thread can shut the context down after spin checked it.
    context = rclpy.context.Context()
    rclpy.init(context=context)
    node = rclpy.create_node('shut_down_before_wait_set', context=context)
    executor = SingleThreadedExecutor(context=context)
    executor.add_node(node)
    context.shutdown()
    try:
        with pytest.raises(ExternalShutdownException):
            executor.spin_once(timeout_sec=0)
    finally:
        node.destroy_node()
