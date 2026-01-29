/**
 * exanic_pybind: ExaNIC C SDK 的 pybind11 封装（仅 Linux）
 *
 * 封装 exanic.h / fifo_rx.h 中的接口，供 Python 调用。ExaNIC SDK 为 C 源码，
 * 由 CMake 编译为静态库并链接进本模块。
 *
 * 暴露接口：acquire_handle, acquire_rx_buffer, receive_frame, release_rx_buffer,
 * release_handle, get_last_error。句柄以 capsule 形式在 Python 间传递。
 */

#include <ctime>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <Python.h>
#include <cstring>
#include <string>

extern "C" {
#include "exanic.h"
#include "fifo_rx.h"
}

namespace py = pybind11;

static const char* CAPSULE_EXANIC = "exanic_t";
static const char* CAPSULE_EXANIC_RX = "exanic_rx_t";

PYBIND11_MODULE(exanic_pybind, m) {
    m.doc() = "ExaNIC C API Python bindings (Linux only)";

    m.def("acquire_handle", [](const std::string& device_name) -> py::object {
        exanic_t* nic = exanic_acquire_handle(device_name.c_str());
        if (!nic)
            return py::none();
        return py::capsule(nic, CAPSULE_EXANIC);
    }, py::arg("device_name"), "Acquire ExaNIC handle. Returns capsule or None.");

    m.def("acquire_rx_buffer", [](py::object handle_cap, int port_number, int buffer_number) -> py::object {
        if (!PyCapsule_IsValid(handle_cap.ptr(), CAPSULE_EXANIC))
            throw std::runtime_error("invalid exanic handle capsule");
        exanic_t* nic = static_cast<exanic_t*>(PyCapsule_GetPointer(handle_cap.ptr(), CAPSULE_EXANIC));
        exanic_rx_t* rx = exanic_acquire_rx_buffer(nic, port_number, buffer_number);
        if (!rx)
            return py::none();
        return py::capsule(rx, CAPSULE_EXANIC_RX);
    }, py::arg("handle"), py::arg("port_number"), py::arg("buffer_number"),
       "Acquire RX buffer. Returns capsule or None.");

    m.def("receive_frame", [](py::object rx_cap, size_t max_size) -> py::bytes {
        if (!PyCapsule_IsValid(rx_cap.ptr(), CAPSULE_EXANIC_RX))
            throw std::runtime_error("invalid exanic_rx handle capsule");
        exanic_rx_t* rx = static_cast<exanic_rx_t*>(PyCapsule_GetPointer(rx_cap.ptr(), CAPSULE_EXANIC_RX));
        if (max_size == 0)
            max_size = 2048;
        std::string buf(max_size, '\0');
        ssize_t n = exanic_receive_frame(rx, &buf[0], max_size, nullptr);
        if (n <= 0)
            return py::bytes("");
        return py::bytes(buf.data(), static_cast<size_t>(n));
    }, py::arg("rx_handle"), py::arg("max_size") = 2048,
       "Receive one frame. Returns frame bytes or empty bytes if none/error.");

    m.def("release_rx_buffer", [](py::object rx_cap) {
        if (!PyCapsule_IsValid(rx_cap.ptr(), CAPSULE_EXANIC_RX))
            return;
        exanic_rx_t* rx = static_cast<exanic_rx_t*>(PyCapsule_GetPointer(rx_cap.ptr(), CAPSULE_EXANIC_RX));
        exanic_release_rx_buffer(rx);
        PyCapsule_SetPointer(rx_cap.ptr(), nullptr);  // avoid double free
    }, py::arg("rx_handle"), "Release RX buffer.");

    m.def("release_handle", [](py::object handle_cap) {
        if (!PyCapsule_IsValid(handle_cap.ptr(), CAPSULE_EXANIC))
            return;
        exanic_t* nic = static_cast<exanic_t*>(PyCapsule_GetPointer(handle_cap.ptr(), CAPSULE_EXANIC));
        exanic_release_handle(nic);
        PyCapsule_SetPointer(handle_cap.ptr(), nullptr);
    }, py::arg("handle"), "Release ExaNIC handle.");

    m.def("get_last_error", []() -> std::string {
        const char* err = exanic_get_last_error();
        return err ? std::string(err) : std::string();
    }, "Get last ExaNIC error message.");
}
