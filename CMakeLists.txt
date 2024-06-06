cmake_minimum_required(VERSION 3.22.1)
project(sort)

add_subdirectory(seastar)

add_executable(sort sort.cpp)
target_link_libraries(sort Seastar::seastar)