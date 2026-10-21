cmake_minimum_required(VERSION 3.8)
project(rclpy_components)

if(NOT CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
endif()
if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

find_package(ament_cmake REQUIRED)
find_package(ament_cmake_python REQUIRED)
find_package(rclcpp REQUIRED)
find_package(rclcpp_components REQUIRED)
find_package(pybind11_vendor REQUIRED)
find_package(Python3 REQUIRED COMPONENTS Interpreter Development)
find_package(pybind11 REQUIRED)

add_executable(component_container_mixed src/component_container_mixed.cpp)
target_link_libraries(component_container_mixed PRIVATE
  rclcpp::rclcpp rclcpp_components::component_manager pybind11::embed)
set_target_properties(component_container_mixed PROPERTIES CXX_VISIBILITY_PRESET hidden)
install(TARGETS component_container_mixed DESTINATION lib/${PROJECT_NAME})

ament_python_install_package(${PROJECT_NAME})
install(PROGRAMS scripts/component_container scripts/component_container_mt
  DESTINATION lib/${PROJECT_NAME})

if(BUILD_TESTING)
  find_package(ament_lint_auto REQUIRED)
  ament_lint_auto_find_test_dependencies()

  find_package(ament_cmake_pytest REQUIRED)
  foreach(_test test_component_manager test_component_container_mixed)
    ament_add_pytest_test(${_test} test/${_test}.py
      ENV MIXED_CONTAINER=$<TARGET_FILE:component_container_mixed>
      APPEND_ENV PYTHONPATH=${CMAKE_CURRENT_SOURCE_DIR}/test:${CMAKE_CURRENT_SOURCE_DIR}
      TIMEOUT 120)
  endforeach()
endif()

ament_package()
