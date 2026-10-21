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

import importlib
from importlib.metadata import entry_points
import re

from composition_interfaces.srv import ListNodes, LoadNode, UnloadNode
from rclpy.executors import Executor
from rclpy.node import Node
from rclpy.parameter import Parameter
from rclpy.qos import QoSProfile
from rclpy.serialization import deserialize_message

ENTRY_POINT_GROUP = 'rclpy_components'
_MODULE_CLASS = re.compile(r'[\w.]+:[\w.]+')


class ComponentError(Exception):
    """A requested component could not be found or constructed."""


def find_component(package_name: str, plugin_name: str) -> type:
    """
    Return the node class a load request names.

    :param plugin_name: ``module:Class``, or the name of an entry point in the
        ``rclpy_components`` group, either ``package::Class`` or ``Class`` of ``package_name``.
    """
    if _MODULE_CLASS.fullmatch(plugin_name):
        module_name, _, attribute = plugin_name.partition(':')
        try:
            target = importlib.import_module(module_name)
            for part in attribute.split('.'):
                target = getattr(target, part)
        except Exception as e:
            raise ComponentError(f"Failed to import '{plugin_name}': {e}") from e
        return target
    names = (plugin_name, f'{package_name}::{plugin_name}')
    for entry_point in entry_points(group=ENTRY_POINT_GROUP):
        if entry_point.name in names:
            try:
                return entry_point.load()
            except Exception as e:
                raise ComponentError(
                    f"Failed to load entry point '{entry_point.name}': {e}") from e
    raise ComponentError(
        f"Component '{plugin_name}' of package '{package_name}' not found")


def node_arguments(request: LoadNode.Request, context) -> dict:
    """Return the Node keyword arguments for a load request, as rclcpp_components builds them."""
    cli_args = ['--ros-args']
    for rule in request.remap_rules:
        cli_args += ['-r', rule]
    if request.node_name:
        cli_args += ['-r', f'__node:={request.node_name}']
    if request.node_namespace:
        cli_args += ['-r', f'__ns:={request.node_namespace}']
    arguments = {
        'context': context,
        'cli_args': cli_args,
        'use_global_arguments': False,
        'parameter_overrides': [Parameter.from_parameter_msg(p) for p in request.parameters],
    }
    for extra in request.extra_arguments:
        if extra.name == 'forward_global_arguments':
            value = Parameter.from_parameter_msg(extra).value
            if not isinstance(value, bool):
                raise ComponentError(
                    "Extra component argument 'forward_global_arguments' must be a boolean")
            arguments['use_global_arguments'] = value
    return arguments


def load_component(request: LoadNode.Request, context) -> Node:
    """Construct the node a load request names."""
    component = find_component(request.package_name, request.plugin_name)
    arguments = node_arguments(request, context)
    try:
        return component(**arguments)
    except Exception as e:
        raise ComponentError(f'Component constructor threw an exception: {e}') from e


def load_serialized_component(request: bytes, context) -> Node:
    """Construct the node a serialized ``LoadNode`` request names."""
    return load_component(deserialize_message(request, LoadNode.Request), context)


class ComponentManager(Node):
    """Serve the container services of rclcpp_components for rclpy nodes."""

    def __init__(self, executor: Executor, name: str = 'ComponentManager', **kwargs):
        super().__init__(name, **kwargs)
        self._component_executor = executor
        self._components = {}
        self._next_id = 1
        load_qos = QoSProfile(depth=200)
        self._load_node_srv = self.create_service(
            LoadNode, '~/_container/load_node', self.on_load_node, qos_profile=load_qos)
        self._unload_node_srv = self.create_service(
            UnloadNode, '~/_container/unload_node', self.on_unload_node, qos_profile=load_qos)
        self._list_nodes_srv = self.create_service(
            ListNodes, '~/_container/list_nodes', self.on_list_nodes)

    def on_list_nodes(self, request: ListNodes.Request, response: ListNodes.Response):
        for unique_id, node in self._components.items():
            response.unique_ids.append(unique_id)
            response.full_node_names.append(node.get_fully_qualified_name())
        return response

    def on_load_node(self, request: LoadNode.Request, response: LoadNode.Response):
        try:
            node = load_component(request, self.context)
        except ComponentError as e:
            self.get_logger().error(str(e))
            response.success = False
            response.error_message = str(e)
            return response
        name = node.get_fully_qualified_name()
        loaded = [n.get_fully_qualified_name() for n in self._components.values()]
        if name == self.get_fully_qualified_name() or name in loaded:
            node.destroy_node()
            response.success = False
            response.error_message = f'Node with name {name} already exists in this process.'
            self.get_logger().error(response.error_message)
            return response
        response.unique_id = self._next_id
        self._next_id += 1
        self._components[response.unique_id] = node
        self._component_executor.add_node(node)
        response.full_node_name = name
        response.success = True
        return response

    def on_unload_node(self, request: UnloadNode.Request, response: UnloadNode.Response):
        node = self._components.pop(request.unique_id, None)
        if node is None:
            response.success = False
            response.error_message = f'No node found with unique_id: {request.unique_id}'
            self.get_logger().warn(response.error_message)
            return response
        self._component_executor.remove_node(node)
        node.destroy_node()
        response.success = True
        return response

    def destroy_node(self):
        for node in self._components.values():
            self._component_executor.remove_node(node)
            node.destroy_node()
        self._components.clear()
        return super().destroy_node()
