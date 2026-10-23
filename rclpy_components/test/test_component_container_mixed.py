# Copyright 2026 nevlife
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
import threading
import time

from composition_interfaces.srv import ListNodes, LoadNode, UnloadNode
import pytest
import rclpy
from rclpy.executors import SingleThreadedExecutor
from std_msgs.msg import String


@pytest.fixture()
def container(tmp_path):
    log = open(tmp_path / 'container.log', 'w+')
    process = subprocess.Popen(
        [os.environ['MIXED_CONTAINER'], '--ros-args', '-r', '__node:=mixed_test'],
        stdout=log, stderr=subprocess.STDOUT)
    yield process, log
    if process.poll() is None:
        process.kill()
        process.wait()


@pytest.fixture()
def client_node():
    context = rclpy.context.Context()
    rclpy.init(context=context)
    node = rclpy.create_node('mixed_test_client', context=context)
    executor = SingleThreadedExecutor(context=context)
    executor.add_node(node)
    thread = threading.Thread(target=executor.spin, daemon=True)
    thread.start()
    yield node
    node.destroy_node()
    executor.shutdown()
    thread.join()
    rclpy.shutdown(context=context)


def wait_for(predicate, what, timeout=10.0):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, what
        time.sleep(0.05)


def test_cpp_and_python_components_in_one_container(container, client_node):
    process, log = container
    load_cli = client_node.create_client(LoadNode, '/mixed_test/_container/load_node')
    unload_cli = client_node.create_client(UnloadNode, '/mixed_test/_container/unload_node')
    list_cli = client_node.create_client(ListNodes, '/mixed_test/_container/list_nodes')
    for client in (load_cli, unload_cli, list_cli):
        assert client.wait_for_service(timeout_sec=10.0)
    heard = []
    client_node.create_subscription(String, 'heard', lambda msg: heard.append(msg.data), 10)

    def load(package_name, plugin_name, **fields):
        return load_cli.call(
            LoadNode.Request(package_name=package_name, plugin_name=plugin_name, **fields))

    relay = load('rclpy_components', 'component_fixtures:Relay')
    assert relay.success, relay.error_message
    talker = load('composition', 'composition::Talker')
    assert talker.success, talker.error_message
    wait_for(lambda: any(h.startswith('relay Hello World: ') for h in heard),
             'Python relay did not receive the C++ talker')

    listener = load('composition', 'composition::Listener')
    assert listener.success, listener.error_message
    py_talker = load('rclpy_components', 'component_fixtures:Talker')
    assert py_talker.success, py_talker.error_message

    def listener_heard_python():
        log.seek(0)
        return 'I heard: [py ' in log.read()
    wait_for(listener_heard_python, 'C++ listener did not receive the Python talker')

    duplicate = load('rclpy_components', 'component_fixtures:FooNode', node_name='talker')
    assert not duplicate.success
    assert 'already exists' in duplicate.error_message
    missing = load('rclpy_components', 'component_fixtures:Missing')
    assert not missing.success
    assert 'Missing' in missing.error_message

    names = list_cli.call(ListNodes.Request())
    assert sorted(zip(names.unique_ids, names.full_node_names)) == [
        (relay.unique_id, '/py_relay'), (talker.unique_id, '/talker'),
        (listener.unique_id, '/listener'), (py_talker.unique_id, '/py_talker')]

    assert unload_cli.call(UnloadNode.Request(unique_id=relay.unique_id)).success
    assert unload_cli.call(UnloadNode.Request(unique_id=talker.unique_id)).success
    names = list_cli.call(ListNodes.Request())
    assert sorted(names.full_node_names) == ['/listener', '/py_talker']
    wait_for(lambda: 'py_relay' not in client_node.get_node_names(),
             'unloaded Python node is still in the graph')

    process.send_signal(signal.SIGINT)
    assert process.wait(timeout=10) == 0
    log.seek(0)
    assert 'Traceback' not in log.read()


def test_python_components_see_the_container_context(container, client_node):
    load_cli = client_node.create_client(LoadNode, '/mixed_test/_container/load_node')
    assert load_cli.wait_for_service(timeout_sec=10.0)
    probes = []
    client_node.create_subscription(String, 'probe', lambda msg: probes.append(msg.data), 10)
    response = load_cli.call(LoadNode.Request(
        package_name='rclpy_components', plugin_name='component_fixtures:OkProbe'))
    assert response.success, response.error_message
    wait_for(lambda: probes, 'no probe message')
    assert probes[0] == 'ok True'


def test_shutdown_survives_a_failing_destroy_node(container, client_node):
    process, log = container
    load_cli = client_node.create_client(LoadNode, '/mixed_test/_container/load_node')
    assert load_cli.wait_for_service(timeout_sec=10.0)
    response = load_cli.call(LoadNode.Request(
        package_name='rclpy_components', plugin_name='component_fixtures:FailingDestroy'))
    assert response.success, response.error_message
    process.send_signal(signal.SIGINT)
    assert process.wait(timeout=10) == 0
    log.seek(0)
    assert 'destroy_node failed' in log.read()


def test_type_adapted_messages_cross_languages(tmp_path, client_node):
    log = open(tmp_path / 'container.log', 'w+')
    process = subprocess.Popen(
        [os.environ['MIXED_CONTAINER'], '--ros-args', '-r', '__node:=mixed_test'],
        stdout=log, stderr=subprocess.STDOUT, env=dict(os.environ, RCL_INTRA_PROCESS='1'))
    try:
        load_cli = client_node.create_client(LoadNode, '/mixed_test/_container/load_node')
        assert load_cli.wait_for_service(timeout_sec=10.0)
        heard = []
        client_node.create_subscription(String, 'heard', lambda msg: heard.append(msg.data), 10)
        for package_name, plugin_name in [
                ('rclpy_components', 'component_fixtures:AdaptedListener'),
                ('composition', 'composition::Talker'),
                ('composition', 'composition::Listener'),
                ('rclpy_components', 'component_fixtures:AdaptedTalker')]:
            response = load_cli.call(
                LoadNode.Request(package_name=package_name, plugin_name=plugin_name))
            assert response.success, response.error_message
        broker = os.environ.get('RMW_IMPLEMENTATION') == 'rmw_zenoh_cpp'
        wait_for(lambda: any(h.startswith('listener Hello World: ') and
                             h.endswith('converted=True') for h in heard),
                 'Python adapted listener did not convert the C++ talker')
        wait_for(lambda: any(h.startswith('listener adapted ') and
                             h.endswith(f'converted={not broker}') for h in heard),
                 'Python adapted listener did not receive the Python adapted talker')

        def cpp_listener_heard_python():
            log.seek(0)
            return 'I heard: [adapted ' in log.read()
        wait_for(cpp_listener_heard_python, 'C++ listener did not receive the adapted talker')
        process.send_signal(signal.SIGINT)
        assert process.wait(timeout=10) == 0
        log.seek(0)
        assert 'Traceback' not in log.read()
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        log.close()
