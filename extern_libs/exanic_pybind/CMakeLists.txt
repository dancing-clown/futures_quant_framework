cmake_minimum_required(VERSION 3.10)
project(exanic_pybind)

set(CMAKE_CXX_STANDARD 11)

# ExaNIC SDK 仅支持 Linux（/dev/exanic*、ioctl、mmap）
if(APPLE)
    message(FATAL_ERROR "exanic_pybind only supports Linux. Current target is macOS. Please build on Linux.")
endif()
if(NOT UNIX)
    message(FATAL_ERROR "exanic_pybind only supports Linux (UNIX).")
endif()

# --- 查找 pybind11（与 ctp_pybind/nsq_pybind 一致） ---
execute_process(
    COMMAND python3 -c "import pybind11; print(pybind11.get_cmake_dir())"
    OUTPUT_VARIABLE pybind11_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
)

if(NOT pybind11_DIR)
    find_package(pybind11 REQUIRED)
else()
    find_package(pybind11 REQUIRED PATHS ${pybind11_DIR} NO_DEFAULT_PATH)
endif()

# --- ExaNIC SDK：使用本目录下 sdk/ 中的头文件与 C 源码（自包含，不依赖 hs-future-gfex-api）---
set(EXANIC_SDK_DIR "${CMAKE_CURRENT_SOURCE_DIR}/sdk")

if(NOT EXISTS "${EXANIC_SDK_DIR}/exanic.c")
    message(FATAL_ERROR "ExaNIC SDK source not found: ${EXANIC_SDK_DIR}\n"
                        "Ensure sdk/ contains exanic.c and other C sources (copied from ExaNIC SDK).")
endif()

# --- 将 ExaNIC C 源码编译为静态库（-fPIC 以便链接进 .so） ---
set(EXANIC_C_SOURCES
    ${EXANIC_SDK_DIR}/exanic.c
    ${EXANIC_SDK_DIR}/config.c
    ${EXANIC_SDK_DIR}/eeprom.c
    ${EXANIC_SDK_DIR}/filter.c
    ${EXANIC_SDK_DIR}/firewall.c
    ${EXANIC_SDK_DIR}/fifo_rx.c
    ${EXANIC_SDK_DIR}/fifo_tx.c
    ${EXANIC_SDK_DIR}/port.c
    ${EXANIC_SDK_DIR}/time.c
    ${EXANIC_SDK_DIR}/transceiver.c
    ${EXANIC_SDK_DIR}/util.c
    ${EXANIC_SDK_DIR}/filter/parser.c
    ${EXANIC_SDK_DIR}/filter/rules.c
)

add_library(exanic_c STATIC ${EXANIC_C_SOURCES})
target_include_directories(exanic_c PUBLIC ${EXANIC_SDK_DIR} ${EXANIC_SDK_DIR}/filter)
set_target_properties(exanic_c PROPERTIES POSITION_INDEPENDENT_CODE ON)

# --- 创建 pybind11 模块 ---
pybind11_add_module(exanic_pybind exanic_pybind.cpp)

target_include_directories(exanic_pybind PRIVATE ${EXANIC_SDK_DIR} ${EXANIC_SDK_DIR}/filter)
target_link_libraries(exanic_pybind PRIVATE exanic_c)

set_target_properties(exanic_pybind PROPERTIES
    INSTALL_RPATH "$ORIGIN"
    BUILD_WITH_INSTALL_RPATH TRUE
)

message(STATUS "ExaNIC SDK source directory: ${EXANIC_SDK_DIR}")
