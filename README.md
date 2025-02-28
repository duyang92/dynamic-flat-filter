# Dynamic Flat Filter (DFF)

In this paper, we present the Dynamic Flat Filter (DFF), a dynamic AMQ framework that empowers fingerprint-based filters (such as Cuckoo Filters) to achieve scalability while guaranteeing constant-time insertions, queries, and deletions, and effectively controls the growth of the false positive rate as the capacity scales.

This repository contains our implementation of the DFF framework applied to Cuckoo Filters with an optional fingerprint growth mechanism in the `src/` directory. Our implementation builds upon the [efficient/cuckoofilter](https://github.com/efficient/cuckoofilter) repository. For usage examples, see `src/main.cpp`.

The `benchmark/` directory includes comprehensive performance evaluations. The benchmark results can be visualized in the IPython notebook `benchmark/benchmark_visualization.ipynb`. We’ve rigorously tested all baseline implementations to ensure benchmark accuracy, with test cases available in the `test/` directory.

## Software Architecture

- Programming Language: C++
- Build Tool: CMake
- Logging: spdlog
- Testing: Catch2
- Others: Clang-Tidy / Clang-Format

## Build

This project uses CMake for building and [CPM.cmake](https://github.com/cpm-cmake/CPM.cmake) to automatically download dependencies during the build, simplifying the build process.

> [!NOTE]
>
> You can download `CPM.cmake` manually if you encounter download failure during the build process. Execute the following command **in the project root directory** if you need to download `CPM.cmake` manually:
>
> ```bash
> mkdir -p build && wget -O build/CPM.cmake https://github.com/cpm-cmake/CPM.cmake/releases/download/v0.40.5/CPM.cmake
> ```

Ensure your C++ compiler supports the C++23 standard before building. This project has been tested with GCC 14 and Clang 19 on macOS/Linux, and with Clang 19 on Windows.

For optimal performance and stability, we recommend using Linux to run this code. All primary development and benchmarking were conducted on Linux environments, and users may encounter platform-specific issues when using alternative operating systems.

**macOS/Linux users** - To build in debug mode, run the following command in the **project root directory**:

```bash
mkdir -p build
cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug ..
cmake --build . --config Debug
cd ..
```

**Windows users** - To build in debug mode, run the following command with **PowerShell** in the **project root directory**:

```powershell
mkdir -Force build | Out-Null
cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug ..
cmake --build . --config Debug
cd ..
```

**macOS/Linux users** - To build in release mode, run the following command in **the project root directory**:

```bash
mkdir -p build
cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release ..
cmake --build . --config Release
cd ..
```

**Windows users** - To build in release mode, run the following command with **PowerShell** in the **project root directory**:

```powershell
mkdir -Force build | Out-Null
cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release ..
cmake --build . --config Release
cd ..
```

## Run

After building, execute the following command **in the project root directory** to run the program:

```shell
./build/DFF
```

## Test

This project uses [Catch2](https://github.com/catchorg/Catch2) for testing. The test cases are located in the `test/` directory. To run the test cases, run the following command **in the project root directory** after building:

```shell
./build/tests
```

## Benchmark

Benchmarks are included in the `benchmark/` directory. To run the benchmark cases, run the following command **in the project root directory** after building:

```shell
./build/benchmarks > benchmark_results.log
```

You can visualize benchmark results using the IPython notebook `benchmark/benchmark_visualization.ipynb`.

Individual benchmarks can be executed via `./build/BM_<benchmark_name>`. For example, to benchmark insertion throughput on DFF:

```shell
./build/BM_insertion_throughput DFF <initial_capacity_log2> <element_count>
```
