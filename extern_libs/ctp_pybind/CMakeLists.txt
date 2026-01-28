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
    # macOS 环境
    set(CTP_SDK_PLATFORM_DIR "${CMAKE_CURRENT_SOURCE_DIR}/macos")
    set(CTP_SDK_INCLUDE_DIR "${CTP_SDK_PLATFORM_DIR}/Headers")
    set(CTP_SDK_LIB_DIR "${CTP_SDK_PLATFORM_DIR}/lib")
    set(CTP_SDK_LIB_NAME "thostmduserapi_se")
    set(CTP_SDK_LIB_FILE "${CTP_SDK_LIB_DIR}/${CTP_SDK_LIB_NAME}")
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
    # macOS: 库文件无 lib 前缀，直接链接文件名
    target_link_libraries(ctp_pybind PRIVATE ${CTP_SDK_LIB_FILE})
    # 设置链接库的路径为 @loader_path（相对于 .so 文件的位置）
    set_target_properties(ctp_pybind PROPERTIES
        INSTALL_RPATH "@loader_path"
        BUILD_WITH_INSTALL_RPATH TRUE
        LINK_FLAGS "-Wl,-rpath,@loader_path"
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

# --- 编译后自动将 SDK 的库文件拷贝到输出目录 ---
if(APPLE)
    add_custom_command(TARGET ctp_pybind POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CTP_SDK_LIB_FILE}"
        $<TARGET_FILE_DIR:ctp_pybind>
        COMMENT "Copying CTP SDK shared library to build directory"
    )
else()
    add_custom_command(TARGET ctp_pybind POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CTP_SDK_LIB_FILE}"
        $<TARGET_FILE_DIR:ctp_pybind>
        COMMENT "Copying CTP SDK shared library to build directory"
    )
endif()
