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

# --- 设置 CTP SDK 路径 ---
set(CTP_SDK_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")
set(CTP_SDK_LIB_DIR "${CMAKE_CURRENT_SOURCE_DIR}/lib")

include_directories(${CTP_SDK_INCLUDE_DIR})
link_directories(${CTP_SDK_LIB_DIR})

# --- 创建 pybind11 模块 ---
pybind11_add_module(ctp_pybind ctp_pybind.cpp)

# --- 链接 CTP 库 ---
target_link_libraries(ctp_pybind PRIVATE Thostmduserapi_se)

# --- 设置 RPATH 确保生成的 .so 能找到 CTP 的 .so ---
if(UNIX AND NOT APPLE)
    set_target_properties(ctp_pybind PROPERTIES INSTALL_RPATH "$ORIGIN")
    set_target_properties(ctp_pybind PROPERTIES BUILD_WITH_INSTALL_RPATH TRUE)
endif()

# --- 编译后自动将 SDK 的 .so 拷贝到输出目录 ---
add_custom_command(TARGET ctp_pybind POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
    "${CTP_SDK_LIB_DIR}/libThostmduserapi_se.so"
    $<TARGET_FILE_DIR:ctp_pybind>
    COMMENT "Copying CTP SDK shared library to build directory"
)
