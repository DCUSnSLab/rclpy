# Copyright 2020 Open Source Robotics Foundation, Inc.
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

import sys
import threading
import time

from composition_interfaces.srv import ListNodes, LoadNode, UnloadNode
import pytest
import rclpy
from rclpy.executors import MultiThreadedExecutor, SingleThreadedExecutor
from rclpy.parameter import Parameter
from rclpy_components.component_manager import ComponentManager

EXECUTORS = (SingleThreadedExecutor, MultiThreadedExecutor)


@pytest.fixture(params=EXECUTORS)
def component_manager(request):
    rclpy.init()
    executor = request.param()
    component_manager = ComponentManager(executor, 'ut_container')
    executor.add_node(component_manager)
    t = threading.Thread(target=executor.spin, daemon=True)
    t.start()

    yield component_manager

    executor.remove_node(component_manager)
    component_manager.destroy_node()
    executor.shutdown()
    t.join()
    rclpy.shutdown()


@pytest.fixture()
def fake_distribution(tmp_path):
    info = tmp_path / 'rclpy_components_fixtures-0.0.dist-info'
    info.mkdir()
    (info / 'METADATA').write_text(
        'Metadata-Version: 2.1\nName: rclpy-components-fixtures\nVersion: 0.0\n')
    (info / 'entry_points.txt').write_text(
        '[rclpy_components]\nfixtures::FooNode = component_fixtures:FooNode\n')
    sys.path.insert(0, str(tmp_path))
    yield
    sys.path.remove(str(tmp_path))


def load(component_manager, package_name, plugin_name, **fields):
    request = LoadNode.Request(package_name=package_name, plugin_name=plugin_name, **fields)
    return component_manager.on_load_node(request, LoadNode.Response())


def list_names(component_manager):
    return component_manager.on_list_nodes(ListNodes.Request(), ListNodes.Response())


def test_list_nodes_no_nodes(component_manager):
    res = list_names(component_manager)
    assert len(res.full_node_names) == 0
    assert len(res.unique_ids) == 0


def test_load_node_no_such_node(component_manager):
    res = load(component_manager, 'fake_package', 'FakePluginName')
    assert 'not found' in res.error_message
    assert res.success is False


def test_load_then_unload_node(component_manager):
    load_res = load(component_manager, 'fixtures', 'component_fixtures:FooNode')
    assert load_res.success is True
    list_res = list_names(component_manager)
    assert list(list_res.unique_ids) == [load_res.unique_id]
    assert list_res.full_node_names == ['/test_foo']

    unload_res = component_manager.on_unload_node(
        UnloadNode.Request(unique_id=load_res.unique_id), UnloadNode.Response())
    assert unload_res.success is True
    assert len(list_names(component_manager).unique_ids) == 0

    deadline = time.monotonic() + 5
    while 'test_foo' in component_manager.get_node_names():
        assert time.monotonic() < deadline, 'unloaded node is still in the graph'
        time.sleep(0.05)


def test_load_entry_point(component_manager, fake_distribution):
    assert load(component_manager, 'fixtures', 'FooNode').success is True
    res = load(component_manager, 'fixtures', 'fixtures::FooNode', node_name='second')
    assert res.success is True
    assert res.full_node_name == '/second'


def test_unload_non_existent_node(component_manager):
    unload_res = component_manager.on_unload_node(
        UnloadNode.Request(unique_id=42), UnloadNode.Response())
    assert unload_res.success is False
    assert '42' in unload_res.error_message


def test_load_fails(component_manager):
    res = load(component_manager, 'fixtures', 'component_fixtures:Missing')
    assert res.success is False
    assert 'Missing' in res.error_message


def test_load_node_wont_init(component_manager):
    res = load(component_manager, 'fixtures', 'component_fixtures:Broken')
    assert res.success is False
    assert 'Oops I failed to construct' in res.error_message


def test_load_new_name_and_namespace(component_manager):
    res = load(component_manager, 'fixtures', 'component_fixtures:FooNode',
               node_name='kitty', node_namespace='/hello')
    assert res.success is True
    assert list_names(component_manager).full_node_names == ['/hello/kitty']


def test_load_same_name_twice(component_manager):
    assert load(component_manager, 'fixtures', 'component_fixtures:FooNode').success is True
    res = load(component_manager, 'fixtures', 'component_fixtures:FooNode')
    assert res.success is False
    assert 'already exists' in res.error_message
    assert len(list_names(component_manager).unique_ids) == 1


def test_load_with_parameters(component_manager):
    res = load(component_manager, 'fixtures', 'component_fixtures:FooNode',
               parameters=[Parameter('answer', value=42).to_parameter_msg()])
    assert res.success is True
    node = component_manager._components[res.unique_id]
    assert node.answer == 42


def test_load_list_unload_via_services(component_manager):
    context = rclpy.context.Context()
    rclpy.init(context=context)
    node = rclpy.create_node('test_node', context=context)
    executor = SingleThreadedExecutor(context=context)
    executor.add_node(node)
    t = threading.Thread(target=executor.spin, daemon=True)
    t.start()
    try:
        manager_fqn = component_manager.get_fully_qualified_name()
        load_cli = node.create_client(LoadNode, f'{manager_fqn}/_container/load_node')
        unload_cli = node.create_client(UnloadNode, f'{manager_fqn}/_container/unload_node')
        list_cli = node.create_client(ListNodes, f'{manager_fqn}/_container/list_nodes')
        for client in (load_cli, unload_cli, list_cli):
            assert client.wait_for_service(timeout_sec=5.0)

        load_res = load_cli.call(LoadNode.Request(
            package_name='fixtures', plugin_name='component_fixtures:FooNode'))
        assert load_res.success is True
        list_res = list_cli.call(ListNodes.Request())
        assert list(list_res.unique_ids) == [load_res.unique_id]
        unload_res = unload_cli.call(UnloadNode.Request(unique_id=load_res.unique_id))
        assert unload_res.success is True
    finally:
        node.destroy_node()
        executor.shutdown()
        t.join()
        rclpy.shutdown(context=context)
