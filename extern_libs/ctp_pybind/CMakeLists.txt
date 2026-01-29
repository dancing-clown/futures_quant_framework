cmake_minimum_required(VERSION 3.10)
project(ctp_pybind)

set(CMAKE_CXX_STANDARD 11)

# --- 查找 pybind11 ---
# 优先尝试从 Python 环境中查找 pybind11
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

# --- 根据编译环境选择 CTP SDK 路径 ---
if(APPLE)
    # macOS 环境：使用 thostmduserapi_se.framework
    set(CTP_SDK_PLATFORM_DIR "${CMAKE_CURRENT_SOURCE_DIR}/macos")
    set(CTP_SDK_FRAMEWORK "${CTP_SDK_PLATFORM_DIR}/thostmduserapi_se.framework")
    set(CTP_SDK_INCLUDE_DIR "${CTP_SDK_FRAMEWORK}/Headers")
    # 给后续通用检查使用：指向 framework 内部实际二进制
    set(CTP_SDK_LIB_DIR "${CTP_SDK_PLATFORM_DIR}")
    set(CTP_SDK_LIB_FILE "${CTP_SDK_FRAMEWORK}/Versions/A/thostmduserapi_se")
elseif(UNIX)
    # Linux 环境
    set(CTP_SDK_PLATFORM_DIR "${CMAKE_CURRENT_SOURCE_DIR}/linux")
    set(CTP_SDK_INCLUDE_DIR "${CTP_SDK_PLATFORM_DIR}/include")
    set(CTP_SDK_LIB_DIR "${CTP_SDK_PLATFORM_DIR}/lib")
    set(CTP_SDK_LIB_NAME "Thostmduserapi_se")
    set(CTP_SDK_LIB_FILE "${CTP_SDK_LIB_DIR}/lib${CTP_SDK_LIB_NAME}.so")
else()
    message(FATAL_ERROR "Unsupported platform. Only macOS and Linux are supported.")
endif()

# 检查 SDK 文件是否存在
if(NOT EXISTS ${CTP_SDK_INCLUDE_DIR})
    message(FATAL_ERROR "CTP SDK include directory not found: ${CTP_SDK_INCLUDE_DIR}")
endif()
if(NOT EXISTS ${CTP_SDK_LIB_FILE})
    message(FATAL_ERROR "CTP SDK library not found: ${CTP_SDK_LIB_FILE}")
endif()

message(STATUS "Using CTP SDK platform: ${CTP_SDK_PLATFORM_DIR}")
message(STATUS "CTP SDK include directory: ${CTP_SDK_INCLUDE_DIR}")
message(STATUS "CTP SDK library: ${CTP_SDK_LIB_FILE}")

include_directories(${CTP_SDK_INCLUDE_DIR})
link_directories(${CTP_SDK_LIB_DIR})

# --- 创建 pybind11 模块 ---
pybind11_add_module(ctp_pybind ctp_pybind.cpp)

# --- 链接 CTP 库 ---
if(APPLE)
    # macOS: 使用 framework 形式链接，并将 framework 拷贝到 build 目录
    target_link_libraries(ctp_pybind PRIVATE "-F${CTP_SDK_PLATFORM_DIR}" "-framework thostmduserapi_se")
    # 运行时从 .so 所在目录作为 rpath 根：@loader_path/thostmduserapi_se.framework/...
    set_target_properties(ctp_pybind PROPERTIES
        INSTALL_RPATH "@loader_path"
        BUILD_WITH_INSTALL_RPATH TRUE
    )
else()
    # Linux: 使用库名（CMake 会自动添加 lib 前缀和 .so 后缀）
    target_link_libraries(ctp_pybind PRIVATE ${CTP_SDK_LIB_NAME})
    # Linux: 使用 $ORIGIN
    set_target_properties(ctp_pybind PROPERTIES
        INSTALL_RPATH "$ORIGIN"
        BUILD_WITH_INSTALL_RPATH TRUE
    )
endif()

# --- 编译后将 SDK 拷贝到输出目录 ---
if(APPLE)
    # 拷贝完整 framework 目录，保证 @rpath/thostmduserapi_se.framework/... 能在 @loader_path 下找到
    add_custom_command(TARGET ctp_pybind POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_directory
        "${CTP_SDK_FRAMEWORK}"
        "$<TARGET_FILE_DIR:ctp_pybind>/thostmduserapi_se.framework"
        COMMENT "Copying CTP SDK framework to build directory"
    )
else()
    add_custom_command(TARGET ctp_pybind POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CTP_SDK_LIB_FILE}"
        $<TARGET_FILE_DIR:ctp_pybind>
        COMMENT "Copying CTP SDK shared library to build directory"
    )
endif()
