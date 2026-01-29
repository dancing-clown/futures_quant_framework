cmake_minimum_required(VERSION 3.10)
project(nsq_pybind)

set(CMAKE_CXX_STANDARD 11)

# nsq-dce-net-api SDK 仅支持 Linux；macOS 直接失败并提示
if(APPLE)
    message(FATAL_ERROR "nsq_pybind only supports Linux. Current target is macOS (APPLE=ON). Please build on Linux.")
endif()
if(NOT UNIX)
    message(FATAL_ERROR "nsq_pybind only supports Linux (UNIX).")
endif()

# --- 查找 pybind11（与 ctp_pybind 一致） ---
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

# --- NSQ SDK 路径（仿照 extern_libs/ctp_pybind 的 linux 结构） ---
set(NSQ_SDK_PLATFORM_DIR "${CMAKE_CURRENT_SOURCE_DIR}/linux")
set(NSQ_SDK_INCLUDE_DIR "${NSQ_SDK_PLATFORM_DIR}/include")
set(NSQ_SDK_LIB_DIR "${NSQ_SDK_PLATFORM_DIR}/lib")
set(NSQ_SDK_LIB_NAME "HSNsqApi")
set(NSQ_SDK_LIB_FILE "${NSQ_SDK_LIB_DIR}/lib${NSQ_SDK_LIB_NAME}.so")

# 检查 SDK 文件是否存在（库文件通常不随仓库提交，需要手工放置）
if(NOT EXISTS ${NSQ_SDK_INCLUDE_DIR})
    message(FATAL_ERROR "NSQ SDK include directory not found: ${NSQ_SDK_INCLUDE_DIR}")
endif()
if(NOT EXISTS ${NSQ_SDK_LIB_FILE})
    message(FATAL_ERROR "NSQ SDK library not found: ${NSQ_SDK_LIB_FILE}\n"
                        "Please put libHSNsqApi.so into ${NSQ_SDK_LIB_DIR} (Linux only).")
endif()

message(STATUS "Using NSQ SDK include directory: ${NSQ_SDK_INCLUDE_DIR}")
message(STATUS "Using NSQ SDK library: ${NSQ_SDK_LIB_FILE}")

include_directories(${NSQ_SDK_INCLUDE_DIR})
link_directories(${NSQ_SDK_LIB_DIR})

# --- 创建 pybind11 模块 ---
pybind11_add_module(nsq_pybind nsq_pybind.cpp)

# --- 链接 NSQ 库（Linux） ---
target_link_libraries(nsq_pybind PRIVATE ${NSQ_SDK_LIB_NAME} pthread rt)
# 仅用 $ORIGIN 作 rpath，与 .so 同目录的 libHSNsqApi.so（POST_BUILD 拷贝）可被正确加载；
# 禁止把 link_directories 的绝对路径写入 rpath。
set_target_properties(nsq_pybind PROPERTIES
    INSTALL_RPATH "$\{ORIGIN\}"
    INSTALL_RPATH_USE_LINK_PATH FALSE
    BUILD_WITH_INSTALL_RPATH TRUE
)

# --- 编译后自动将 SDK 的库文件拷贝到输出目录 ---
add_custom_command(TARGET nsq_pybind POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
    "${NSQ_SDK_LIB_FILE}"
    $<TARGET_FILE_DIR:nsq_pybind>
    COMMENT "Copying NSQ SDK shared library to build directory"
)

