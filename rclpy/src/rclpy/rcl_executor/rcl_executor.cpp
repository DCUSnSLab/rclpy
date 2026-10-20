// Copyright 2026 nev
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
// writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#include <pybind11/pybind11.h>
#include <rcl_executor/executor.h>
#include <rcl/error_handling.h>
#include <functional>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>
#include "client.hpp"
#include "events_executor/scoped_with.hpp"
#include "exceptions.hpp"
#include "guard_condition.hpp"
#include "service.hpp"
#include "signal_handler.hpp"
#include "subscription.hpp"
#include "timer.hpp"
#include "wait_set.hpp"

namespace py = pybind11;
namespace rclpy
{
namespace
{
void checked(rcl_ret_t result)
{
  if (result != RCL_RET_OK) {throw RCLError("rcl executor operation failed");}
}

class RclExecutorCore
{
  struct View
  {
    py::object waitset;
    std::function<void(bool)> set_ready;
  };
  rcl_executor_t * core_ = nullptr;
  rcl_executor_handle_t * handle_ = nullptr;
  std::unique_ptr<ScopedSignalCallback> signal_;
  std::unordered_map<uint64_t, std::shared_ptr<events_executor::ScopedWith>> lifetimes_;
  std::unordered_map<uint64_t, View> views_;
  std::unordered_map<void *, std::unique_ptr<py::object>> posted_;
  // Subscriptions whose callback is a plain function, keyed by registration, with that callback.
  std::unordered_map<uint64_t, std::pair<py::object, py::object>> plain_;

public:
  explicit RclExecutorCore(int policy)
  {
    checked(rcl_executor_create_with_policy(
        &core_, static_cast<rcl_executor_selection_policy_t>(policy)));
    handle_ = rcl_executor_get_handle(core_);
    signal_ = std::make_unique<ScopedSignalCallback>([this] {rcl_executor_wake(handle_);});
  }
  ~RclExecutorCore()
  {
    close();
    rcl_executor_handle_destroy(handle_);
  }
  void close()
  {
    if (!core_) {return;}
    signal_.reset();
    rcl_executor_destroy(core_);
    core_ = nullptr;
    views_.clear();
    plain_.clear();
    lifetimes_.clear();
    posted_.clear();
  }
  void wake() {rcl_executor_wake(handle_);}
  void stop() {rcl_executor_stop(handle_);}
  void group(uint64_t id, bool reentrant)
  {
    checked(rcl_executor_add_group(core_, id,
      reentrant ? RCL_EXECUTOR_REENTRANT : RCL_EXECUTOR_MUTUALLY_EXCLUSIVE));
  }
  void priority(uint64_t id, int32_t value)
  {
    checked(rcl_executor_set_group_priority(core_, id, value));
  }
  uint64_t add_raw(int type, const void * pointer, uintptr_t context, uint64_t group_id)
  {
    uint64_t id;
    checked(rcl_executor_add(core_, static_cast<rcl_executor_entity_type_t>(type), pointer,
      reinterpret_cast<void *>(context), group_id, &id));
    return id;
  }
  uint64_t add(int type, py::object handle, uintptr_t context, uint64_t group_id)
  {
    auto lifetime = std::make_shared<events_executor::ScopedWith>(handle);
    const void * pointer;
    switch (type) {
      case RCL_EXECUTOR_SUBSCRIPTION: pointer = handle.cast<Subscription &>().rcl_ptr(); break;
      case RCL_EXECUTOR_TIMER: pointer = handle.cast<Timer &>().rcl_ptr(); break;
      case RCL_EXECUTOR_SERVICE: pointer = handle.cast<Service &>().rcl_ptr(); break;
      case RCL_EXECUTOR_CLIENT: pointer = handle.cast<Client &>().rcl_ptr(); break;
      case RCL_EXECUTOR_EVENT: pointer = handle.cast<EventHandle &>().rcl_ptr(); break;
      case RCL_EXECUTOR_GUARD_CONDITION: pointer = handle.cast<GuardCondition &>().rcl_ptr(); break;
      default: throw py::value_error("unsupported entity type");
    }
    auto id = add_raw(type, pointer, context, group_id);
    lifetimes_.emplace(id, std::move(lifetime));
    return id;
  }
  py::list add_waitable(
    py::object waitable, py::object waitset, uintptr_t context, uint64_t group_id)
  {
    py::list ids;
    auto lifetime = std::make_shared<events_executor::ScopedWith>(waitable);
    auto ws = waitset.cast<WaitSet &>().rcl_ptr();
    auto collect = [&](auto array, size_t size, int type) {
        for (size_t i = 0; i < size; ++i) {
          if (!array[i]) {continue;}
          auto pointer = array[i];
          auto id = add_raw(type, pointer, context, group_id);
          lifetimes_.emplace(id, lifetime);
          views_.emplace(id, View{waitset, [array, i, pointer](bool ready) {
                array[i] = ready ? pointer : nullptr;
              }});
          array[i] = nullptr;
          ids.append(id);
        }
      };
    collect(ws->subscriptions, ws->size_of_subscriptions, RCL_EXECUTOR_SUBSCRIPTION);
    collect(ws->timers, ws->size_of_timers, RCL_EXECUTOR_TIMER_OBSERVER);
    collect(ws->services, ws->size_of_services, RCL_EXECUTOR_SERVICE);
    collect(ws->clients, ws->size_of_clients, RCL_EXECUTOR_CLIENT);
    collect(ws->events, ws->size_of_events, RCL_EXECUTOR_EVENT);
    collect(ws->guard_conditions, ws->size_of_guard_conditions, RCL_EXECUTOR_GUARD_CONDITION);
    return ids;
  }
  void remove(uint64_t id)
  {
    checked(rcl_executor_remove(core_, id));
    views_.erase(id);
    plain_.erase(id);
    lifetimes_.erase(id);
  }
  int wait(int64_t ns)
  {
    py::gil_scoped_release release;
    auto result = rcl_executor_wait(core_, ns);
    if (result == RCL_EXECUTOR_ERROR) {throw RCLError("rcl executor wait failed");}
    return result;
  }
  void post(py::object work)
  {
    auto owned = std::make_unique<py::object>(std::move(work));
    void * context = owned.get();
    posted_.emplace(context, std::move(owned));
    checked(rcl_executor_post(core_, nullptr, context, 0));
  }
  void set_plain(uint64_t id, py::object subscription, py::object callback)
  {
    plain_[id] = {std::move(subscription), std::move(callback)};
  }
  // Take and call up to limit plain subscription callbacks without Python Task machinery.
  // Returns how many ran and the first other item claimed, which the caller must handle.
  py::tuple dispatch(size_t limit)
  {
    size_t count = 0;
    while (count < limit) {
      rcl_executor_item_t item;
      auto result = rcl_executor_next(core_, &item);
      if (result == RCL_EXECUTOR_ERROR) {throw RCLError("rcl executor claim failed");}
      if (result != RCL_EXECUTOR_READY) {break;}
      auto found = plain_.find(item.registration);
      py::object callback;
      if (found != plain_.end()) {
        callback = found->second.first.attr("callback");
      }
      if (!callback || !callback.is(found->second.second)) {
        return py::make_tuple(count, claimed(item));
      }
      ++count;
      try {
        call_plain(found->second.first, callback);
      } catch (...) {
        complete(item.token);
        throw;
      }
      complete(item.token);
    }
    return py::make_tuple(count, py::none());
  }
  py::object next()
  {
    rcl_executor_item_t item;
    auto result = rcl_executor_next(core_, &item);
    if (result == RCL_EXECUTOR_ERROR) {throw RCLError("rcl executor claim failed");}
    if (result != RCL_EXECUTOR_READY) {return py::none();}
    return claimed(item);
  }

private:
  void call_plain(const py::object & subscription, const py::object & callback)
  {
    auto & handle = subscription.attr("handle").cast<Subscription &>();
    py::object info = handle.take_message(subscription.attr("msg_type"),
        subscription.attr("raw").cast<bool>());
    if (info.is_none()) {return;}
    auto message_only = subscription.attr("CallbackType").attr("MessageOnly");
    if (subscription.attr("_callback_type").is(message_only)) {
      callback(info.cast<py::tuple>()[0]);
    } else {
      callback(*info);
    }
  }
  py::object claimed(const rcl_executor_item_t & item)
  {
    auto posted = posted_.find(item.context);
    if (item.type == RCL_EXECUTOR_WORK && posted != posted_.end()) {
      py::object work = *posted->second;
      posted_.erase(posted);
      return py::make_tuple(work, item.token, static_cast<int>(item.type));
    }
    auto found = views_.find(item.registration);
    if (found != views_.end()) {
      for (auto & view : views_) {
        if (view.second.waitset.is(found->second.waitset)) {view.second.set_ready(false);}
      }
      found->second.set_ready(true);
    }
    return py::make_tuple(reinterpret_cast<uintptr_t>(item.context), item.token,
          static_cast<int>(item.type));
  }

public:
  void complete(uint64_t token) {checked(rcl_executor_complete(core_, token));}
};
}  // namespace

void define_rcl_executor(py::module_ & module)
{
  py::class_<RclExecutorCore>(module, "RclExecutorCore")
  .def(py::init<int>(), py::arg("selection_policy") = 0)
  .def("set_group_priority", &RclExecutorCore::priority)
  .def("add_group", &RclExecutorCore::group)
  .def("add", &RclExecutorCore::add)
  .def("add_waitable", &RclExecutorCore::add_waitable)
  .def("remove", &RclExecutorCore::remove)
  .def("wait", &RclExecutorCore::wait)
  .def("next", &RclExecutorCore::next)
  .def("set_plain", &RclExecutorCore::set_plain)
  .def("dispatch", &RclExecutorCore::dispatch)
  .def("post", &RclExecutorCore::post)
  .def("complete", &RclExecutorCore::complete)
  .def("wake", &RclExecutorCore::wake)
  .def("stop", &RclExecutorCore::stop)
  .def("close", &RclExecutorCore::close);
}
}  // namespace rclpy
