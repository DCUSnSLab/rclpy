# Copyright 2026 nev
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import time
import unittest

import rclpy
from rclpy.action import ActionClient, ActionServer, CancelResponse
from rclpy.callback_groups import ReentrantCallbackGroup
from rclpy.context import Context
from rclpy.executors import SingleThreadedExecutor
from rclpy.task import Future
from test_msgs.action import Fibonacci
from test_msgs.srv import BasicTypes


class TestIntraProcessServices(unittest.TestCase):

    def setUp(self):
        self.previous = os.environ.get('RCL_INTRA_PROCESS')
        os.environ['RCL_INTRA_PROCESS'] = '1'
        self.context = Context()
        rclpy.init(context=self.context)
        self.node = rclpy.create_node('broker_service_py', context=self.context)
        self.executor = SingleThreadedExecutor(context=self.context)
        self.executor.add_node(self.node)

    def tearDown(self):
        self.executor.shutdown()
        self.node.destroy_node()
        self.context.shutdown()
        if self.previous is None:
            os.environ.pop('RCL_INTRA_PROCESS', None)
        else:
            os.environ['RCL_INTRA_PROCESS'] = self.previous

    def result(self, future):
        self.executor.spin_until_future_complete(future, timeout_sec=5)
        self.assertTrue(future.done())
        return future.result()

    def test_multiple_clients_and_core_executor(self):
        from rclpy.experimental import RclExecutor
        calls = []

        def callback(request, response):
            calls.append(request.int64_value)
            response.string_value = request.string_value
            return response

        self.node.create_service(BasicTypes, 'broker_py', callback)
        clients = [self.node.create_client(BasicTypes, 'broker_py') for _ in range(3)]
        self.assertTrue(all(client.wait_for_service(timeout_sec=5) for client in clients))
        for core in (False, True):
            executor = self.executor
            if core:
                self.executor.remove_node(self.node)
                executor = RclExecutor(context=self.context)
                executor.add_node(self.node)
            try:
                futures = [client.call_async(BasicTypes.Request(
                    int64_value=i, string_value='x' * 1024)) for i, client in enumerate(clients)]
                for future in futures:
                    executor.spin_until_future_complete(future, timeout_sec=5)
                    self.assertTrue(future.done())
                    self.assertEqual('x' * 1024, future.result().string_value)
            finally:
                if core:
                    executor.shutdown()
        self.assertEqual([0, 0, 1, 1, 2, 2], sorted(calls))

    def test_action_goal_feedback_result_and_cancel(self):
        release = Future()
        started = []
        feedback = []

        async def execute(goal):
            goal.publish_feedback(Fibonacci.Feedback(sequence=[0, 1]))
            started.append(goal.request.order)
            if goal.request.order == 2:
                await release
                self.assertTrue(goal.is_cancel_requested)
                goal.canceled()
            else:
                goal.succeed()
            return Fibonacci.Result(sequence=[0, 1, 1])

        group = ReentrantCallbackGroup()
        server = ActionServer(
            self.node, Fibonacci, 'broker_action', execute,
            callback_group=group, cancel_callback=lambda goal: CancelResponse.ACCEPT)
        client = ActionClient(self.node, Fibonacci, 'broker_action', callback_group=group)
        try:
            self.assertTrue(client.wait_for_server(timeout_sec=5))
            goal = self.result(client.send_goal_async(
                Fibonacci.Goal(order=1), feedback_callback=lambda msg: feedback.append(msg)))
            self.assertTrue(goal.accepted)
            result = self.result(goal.get_result_async())
            self.assertEqual([0, 1, 1], list(result.result.sequence))
            goal = self.result(client.send_goal_async(Fibonacci.Goal(order=2)))
            self.assertTrue(goal.accepted)
            deadline = time.monotonic() + 5
            while 2 not in started and time.monotonic() < deadline:
                self.executor.spin_once(timeout_sec=0.1)
            self.assertIn(2, started)
            self.assertEqual(1, len(self.result(goal.cancel_goal_async()).goals_canceling))
            release.set_result(True)
            result = self.result(goal.get_result_async())
            self.assertEqual(5, result.status)
            self.assertTrue(feedback)
        finally:
            client.destroy()
            server.destroy()
