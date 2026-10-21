// Copyright 2026 nevlife
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <pybind11/embed.h>

#include <map>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "rclcpp/rclcpp.hpp"
#include "rclcpp/serialization.hpp"
#include "rclcpp_components/component_manager.hpp"

namespace py = pybind11;

namespace rclpy_components
{

/// Component manager that loads rclcpp components and rclpy nodes into one rcl context.
/**
 * Plugins registered in the rclcpp_components ament index load as with ComponentManager.
 * Any other plugin is loaded by rclpy_components.component_manager.load_component and spun
 * by an rclpy executor on its own thread, in a context borrowed from this node's context.
 */
class MixedComponentManager : public rclcpp_components::ComponentManager
{
public:
  using rclcpp_components::ComponentManager::ComponentManager;

  ~MixedComponentManager() override
  {
    if (!thread_.joinable()) {
      return;
    }
    {
      py::gil_scoped_acquire gil;
      executor_py_.attr("shutdown")();
    }
    thread_.join();
    py::gil_scoped_acquire gil;
    for (auto & entry : python_nodes_) {
      entry.second.second.attr("destroy_node")();
    }
    python_nodes_.clear();
    context_py_.attr("destroy")();
    executor_py_ = py::object();
    context_py_ = py::object();
    load_ = py::object();
    external_shutdown_ = py::object();
  }

protected:
  void
  on_load_node(
    const std::shared_ptr<rmw_request_id_t> request_header,
    const std::shared_ptr<LoadNode::Request> request,
    std::shared_ptr<LoadNode::Response> response) override
  {
    if (is_cpp_component(*request)) {
      ComponentManager::on_load_node(request_header, request, response);
      return;
    }
    rclcpp::SerializedMessage serialized;
    rclcpp::Serialization<LoadNode::Request>().serialize_message(request.get(), &serialized);
    const auto & buffer = serialized.get_rcl_serialized_message();
    py::gil_scoped_acquire gil;
    try {
      if (!thread_.joinable()) {
        start_python();
      }
      py::object node = load_(
        py::bytes(reinterpret_cast<const char *>(buffer.buffer), buffer.buffer_length),
        context_py_);
      auto name = node.attr("get_fully_qualified_name")().cast<std::string>();
      if (name_in_use(name)) {
        node.attr("destroy_node")();
        response->success = false;
        response->error_message = "Node with name " + name + " already exists in this process.";
        RCLCPP_ERROR(get_logger(), "%s", response->error_message.c_str());
        return;
      }
      executor_py_.attr("add_node")(node);
      response->unique_id = unique_id_++;
      response->full_node_name = name;
      response->success = true;
      python_nodes_.emplace(response->unique_id, std::make_pair(name, std::move(node)));
    } catch (py::error_already_set & e) {
      response->success = false;
      response->error_message = py::str(e.value());
      RCLCPP_ERROR(get_logger(), "%s", response->error_message.c_str());
    }
  }

  void
  on_unload_node(
    const std::shared_ptr<rmw_request_id_t> request_header,
    const std::shared_ptr<UnloadNode::Request> request,
    std::shared_ptr<UnloadNode::Response> response) override
  {
    auto entry = python_nodes_.find(request->unique_id);
    if (entry == python_nodes_.end()) {
      ComponentManager::on_unload_node(request_header, request, response);
      return;
    }
    py::gil_scoped_acquire gil;
    py::object node = std::move(entry->second.second);
    python_nodes_.erase(entry);
    try {
      executor_py_.attr("remove_node")(node);
      node.attr("destroy_node")();
      response->success = true;
    } catch (py::error_already_set & e) {
      response->success = false;
      response->error_message = py::str(e.value());
      RCLCPP_ERROR(get_logger(), "%s", response->error_message.c_str());
    }
  }

  void
  on_list_nodes(
    const std::shared_ptr<rmw_request_id_t> request_header,
    const std::shared_ptr<ListNodes::Request> request,
    std::shared_ptr<ListNodes::Response> response) override
  {
    ComponentManager::on_list_nodes(request_header, request, response);
    for (const auto & entry : python_nodes_) {
      response->unique_ids.push_back(entry.first);
      response->full_node_names.push_back(entry.second.first);
    }
  }

private:
  bool
  is_cpp_component(const LoadNode::Request & request) const
  {
    try {
      for (const auto & resource : get_component_resources(request.package_name)) {
        if (resource.first == request.plugin_name) {
          return true;
        }
      }
    } catch (const rclcpp_components::ComponentManagerException &) {
    }
    return false;
  }

  bool
  name_in_use(const std::string & name)
  {
    if (name == get_fully_qualified_name()) {
      return true;
    }
    for (auto & wrapper : node_wrappers_) {
      if (name == wrapper.second.get_node_base_interface()->get_fully_qualified_name()) {
        return true;
      }
    }
    for (const auto & entry : python_nodes_) {
      if (name == entry.second.first) {
        return true;
      }
    }
    return false;
  }

  // Called with the GIL held.
  void
  start_python()
  {
    auto lease = std::make_unique<std::shared_ptr<rcl_context_t>>(
      get_node_base_interface()->get_context()->get_rcl_context());
    py::capsule capsule(
      lease.get(), "rcl_context_shared_ptr", [](PyObject * object) {
        delete static_cast<std::shared_ptr<rcl_context_t> *>(
          PyCapsule_GetPointer(object, "rcl_context_shared_ptr"));
      });
    lease.release();
    py::object context = py::module_::import("rclpy.context").attr("Context")();
    context.attr("init")(py::arg("context_lease") = capsule, py::arg("initialize_logging") = false);
    auto executors = py::module_::import("rclpy.executors");
    executor_py_ = executors.attr("SingleThreadedExecutor")(py::arg("context") = context);
    external_shutdown_ = executors.attr("ExternalShutdownException");
    load_ = py::module_::import("rclpy_components.component_manager")
      .attr("load_serialized_component");
    context_py_ = context;
    thread_ = std::thread([this] {spin_python();});
  }

  void
  spin_python()
  {
    py::gil_scoped_acquire gil;
    try {
      executor_py_.attr("spin")();
    } catch (py::error_already_set & e) {
      if (e.matches(external_shutdown_)) {
        return;
      }
      // A Python callback raised: stop the container as an rclcpp callback exception would.
      e.restore();
      PyErr_Print();
      rclcpp::shutdown();
    }
  }

  py::object context_py_;
  py::object executor_py_;
  py::object load_;
  py::object external_shutdown_;
  std::map<uint64_t, std::pair<std::string, py::object>> python_nodes_;
  std::thread thread_;
};

}  // namespace rclpy_components

int main(int argc, char * argv[])
{
  rclcpp::init(argc, argv);
  // rclcpp owns SIGINT; the interpreter must not install its own handler.
  py::initialize_interpreter(false);
  {
    py::gil_scoped_release release;
    auto exec = std::make_shared<rclcpp::executors::SingleThreadedExecutor>();
    auto node = std::make_shared<rclpy_components::MixedComponentManager>(exec);
    exec->add_node(node);
    exec->spin();
  }
  py::finalize_interpreter();
}
