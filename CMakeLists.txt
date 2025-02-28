cmake_minimum_required(VERSION 3.28)

# Set the project name
project(DFF)

# Specify the C++ standard
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Enable ASan to detect memory errors on *nix
if(CMAKE_BUILD_TYPE MATCHES Debug AND UNIX)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
  set(CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} -fsanitize=address")
endif()

# Export compilation database
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Initialize 'projects' variable
set(projects ${PROJECT_NAME})

# Add the executable
file(GLOB_RECURSE SOURCES "src/*.c" "src/*.h" "src/*.cpp" "src/*.hpp" "src/*.tpp" "src/*.cppm")
list(FILTER SOURCES EXCLUDE REGEX ".*[/\\\\]\\.[^/\\\\]+[/\\\\].*") # Filter out dirs starting with '.'
file(GLOB_RECURSE BENCHMARK_IMPL_SOURCES "benchmark/impl/*.h" "benchmark/impl/*.c" "benchmark/impl/*.hpp" "benchmark/impl/*.cpp" "benchmark/impl/*.tpp" "benchmark/impl/*.cppm")
list(FILTER BENCHMARK_IMPL_SOURCES EXCLUDE REGEX ".*[/\\\\]\\.[^/\\\\]+[/\\\\].*") # Filter out dirs starting with '.'
add_executable(${PROJECT_NAME} ${SOURCES} ${BENCHMARK_IMPL_SOURCES})
set(SOURCES_WITHOUT_MAIN ${SOURCES})
list(REMOVE_ITEM SOURCES_WITHOUT_MAIN ${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp)

# Use IWYU if found
find_program(IWYU_PATH NAMES include-what-you-use iwyu)

if(IWYU_PATH)
  message(STATUS "Found Include-What-You-Use (IWYU): ${IWYU_PATH}")
  set(IWYU_OPTIONS
    "-Xiwyu" "--mapping_file=${CMAKE_CURRENT_SOURCE_DIR}/iwyu.imp"
    "-Xiwyu" "--quoted_includes_first"
    "-Xiwyu" "--cxx17ns"
    "-w"
  )
  set_property(TARGET ${PROJECT_NAME} PROPERTY CXX_INCLUDE_WHAT_YOU_USE "${IWYU_PATH};${IWYU_OPTIONS}")
else()
  message(WARNING "Include-What-You-Use (IWYU) not found! Install it to improve the codebase.")
endif()

# Install CPM.cmake
set(CPM_DOWNLOAD_VERSION 0.40.5)
set(CPM_DOWNLOAD_LOCATION "${CMAKE_BINARY_DIR}/CPM.cmake")

if(NOT(EXISTS ${CPM_DOWNLOAD_LOCATION}))
  message(STATUS "Downloading CPM.cmake v${CPM_DOWNLOAD_VERSION} from GitHub")
  file(
    DOWNLOAD
    https://github.com/cpm-cmake/CPM.cmake/releases/download/v${CPM_DOWNLOAD_VERSION}/CPM.cmake
    ${CPM_DOWNLOAD_LOCATION}
    STATUS DOWNLOAD_STATUS
  )
  list(GET DOWNLOAD_STATUS 0 STATUS_CODE)
  list(GET DOWNLOAD_STATUS 1 ERROR_MESSAGE)

  if(NOT STATUS_CODE EQUAL 0)
    file(REMOVE ${CPM_DOWNLOAD_LOCATION})
    message(FATAL_ERROR "Error while downloading CPM.cmake: ${ERROR_MESSAGE}")
  else()
    message(STATUS "Downloaded CPM.cmake")
  endif()
endif()

include(${CPM_DOWNLOAD_LOCATION})

# Testing
if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
  file(GLOB_RECURSE TEST_SOURCES "test/*.cpp")

  CPMAddPackage("gh:catchorg/Catch2@3.7.1")

  if(SOURCES_WITHOUT_MAIN)
    add_executable(tests ${SOURCES_WITHOUT_MAIN} ${TEST_SOURCES} ${BENCHMARK_IMPL_SOURCES})
    target_link_libraries(tests PRIVATE Catch2::Catch2WithMain)

    list(APPEND CMAKE_MODULE_PATH ${CPM_PACKAGE_Catch2_SOURCE_DIR}/extras)
    include(CTest)
    include(Catch)
    catch_discover_tests(tests)

    list(APPEND projects tests)
  endif()
endif()

# Benchmark
if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
  file(GLOB_RECURSE BENCHMARK_SOURCES "benchmark/*.h" "benchmark/*.c" "benchmark/*.hpp" "benchmark/*.cpp" "benchmark/*.tpp" "benchmark/*.cppm")
  list(REMOVE_ITEM BENCHMARK_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/benchmark/benchmark.cpp)
  list(FILTER BENCHMARK_SOURCES EXCLUDE REGEX "benchmark/benchmarks/.*")

  set(REPROC++ ON)
  CPMAddPackage("gh:DaanDeMeyer/reproc@14.2.5")

  file(GLOB BENCHMARK_TASKS "benchmark/benchmarks/*.cpp")
  file(GLOB BENCHMARK_TASK_UTILS "benchmark/benchmarks/*.hpp")

  foreach(BENCHMARK_TASK IN LISTS BENCHMARK_TASKS)
    get_filename_component(BENCHMARK_TASK_NAME ${BENCHMARK_TASK} NAME_WE)
    add_executable(BM_${BENCHMARK_TASK_NAME} ${BENCHMARK_TASK_UTILS} ${BENCHMARK_TASK} ${BENCHMARK_SOURCES} ${SOURCES_WITHOUT_MAIN})
    list(APPEND projects BM_${BENCHMARK_TASK_NAME})
  endforeach()

  add_executable(benchmarks benchmark/benchmark.cpp ${BENCHMARK_SOURCES} ${SOURCES_WITHOUT_MAIN})
  target_link_libraries(benchmarks PRIVATE reproc++)
  list(APPEND projects benchmarks)
endif()

# Packages
CPMAddPackage("gh:Dobiasd/FunctionalPlus@0.2.25")
CPMAddPackage("gh:fmtlib/fmt#10.2.1")
CPMAddPackage("gh:gabime/spdlog@1.14.1")
CPMAddPackage(
  NAME xxHash
  VERSION 0.8.2
  GITHUB_REPOSITORY Cyan4973/xxHash
  SOURCE_SUBDIR cmake_unofficial
)

# Link libraries
foreach(project IN LISTS projects)
  target_link_libraries(${project} PRIVATE FunctionalPlus::fplus)
  target_link_libraries(${project} PRIVATE fmt::fmt)
  target_link_libraries(${project} PRIVATE spdlog::spdlog)
  target_link_libraries(${project} PRIVATE xxHash::xxhash)
endforeach()
