# NSQ SDK Linux 库放置说明

`nsq_pybind` 仅支持 **Linux**。请将 NSQ SDK 的动态库放入本目录，以便 CMake 链接与运行时加载：

- **必需**：`libHSNsqApi.so`
- **可能需要**：SDK 附带的其他 `.so` 依赖（若有）

目录结构应为：

```text
extern_libs/nsq_pybind/
  linux/
    include/
    lib/
      libHSNsqApi.so
      (other .so ...)
```

构建方式（Linux）：

```bash
cd extern_libs/nsq_pybind
mkdir -p build && cd build
cmake ..
make
```

构建产物 `nsq_pybind*.so` 会在 `build/` 下生成，同时 CMake 会把 `libHSNsqApi.so` 拷贝到同目录，便于运行时通过 `$ORIGIN` 加载。
