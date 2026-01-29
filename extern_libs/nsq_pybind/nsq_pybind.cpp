#include <pybind11/pybind11.h>
#include <pybind11/functional.h>
#include <pybind11/stl.h>

// 通过 linux/include 下的“转发头”引入 NSQ SDK 头文件
#include "HSNsqApi.h"

#include <cstring>
#include <string>
#include <vector>

namespace py = pybind11;

// --- SPI 包装类：将 SDK 回调转发给 Python ---
class PyNsqSpi : public CHSNsqSpi {
public:
    using CHSNsqSpi::CHSNsqSpi;

    void OnFrontConnected() override {
        PYBIND11_OVERLOAD(void, CHSNsqSpi, OnFrontConnected);
    }

    void OnFrontDisconnected(int nResult) override {
        PYBIND11_OVERLOAD(void, CHSNsqSpi, OnFrontDisconnected, nResult);
    }

    void OnRspUserLogin(CHSNsqRspUserLoginField *pRspUserLogin, CHSNsqRspInfoField *pRspInfo, int nRequestID, bool bIsLast) override {
        PYBIND11_OVERLOAD(void, CHSNsqSpi, OnRspUserLogin, pRspUserLogin, pRspInfo, nRequestID, bIsLast);
    }

    void OnRspFutuDepthMarketDataSubscribe(CHSNsqRspInfoField *pRspInfo, int nRequestID, bool bIsLast) override {
        PYBIND11_OVERLOAD(void, CHSNsqSpi, OnRspFutuDepthMarketDataSubscribe, pRspInfo, nRequestID, bIsLast);
    }

    void OnRtnFutuDepthMarketData(CHSNsqFutuDepthMarketDataField *pFutuDepthMarketData) override {
        PYBIND11_OVERLOAD(void, CHSNsqSpi, OnRtnFutuDepthMarketData, pFutuDepthMarketData);
    }
};

static void copy_cstr(char *dest, size_t dest_size, const std::string &src) {
    std::memset(dest, 0, dest_size);
    std::strncpy(dest, src.c_str(), dest_size - 1);
}

// --- API 包装类 ---
class PyNsqApi {
public:
    PyNsqApi(const std::string &flow_path = "./log/", const std::string &sdk_cfg_file_path = "")
    : api_(nullptr) {
        if (sdk_cfg_file_path.empty()) {
            api_ = NewNsqApi(flow_path.c_str());
        } else {
            api_ = NewNsqApiExt(flow_path.c_str(), sdk_cfg_file_path.c_str());
        }
    }

    ~PyNsqApi() {
        // SDK 语义：ReleaseApi 删除接口对象本身
        if (api_) {
            api_->ReleaseApi();
            api_ = nullptr;
        }
    }

    void RegisterSpi(PyNsqSpi *spi) {
        if (api_) api_->RegisterSpi(spi);
    }

    int RegisterFront(const std::string &front) {
        return api_ ? api_->RegisterFront(front.c_str()) : -1;
    }

    int Init(const std::string &lic_file, const std::string &safe_level = "", const std::string &pwd = "", const std::string &ssl_file = "", const std::string &ssl_pwd = "") {
        return api_ ? api_->Init(lic_file.c_str(), safe_level.c_str(), pwd.c_str(), ssl_file.c_str(), ssl_pwd.c_str()) : -1;
    }

    int ReqUserLogin(CHSNsqReqUserLoginField *req, int request_id) {
        return api_ ? api_->ReqUserLogin(req, request_id) : -1;
    }

    std::string GetApiErrorMsg(int err) {
        if (!api_) return std::string();
        const char *msg = api_->GetApiErrorMsg(err);
        return msg ? std::string(msg) : std::string();
    }

    const char* GetApiVersion() {
        return GetNsqApiVersion();
    }

    int ReqFutuDepthMarketDataSubscribe(const std::vector<std::pair<std::string, std::string>> &contracts, int request_id) {
        if (!api_) return -1;
        std::vector<CHSNsqReqFutuDepthMarketDataField> reqs;
        reqs.resize(contracts.size());
        for (size_t i = 0; i < contracts.size(); i++) {
            copy_cstr(reqs[i].ExchangeID, sizeof(reqs[i].ExchangeID), contracts[i].first);
            copy_cstr(reqs[i].InstrumentID, sizeof(reqs[i].InstrumentID), contracts[i].second);
        }
        return api_->ReqFutuDepthMarketDataSubscribe(reqs.data(), (int)reqs.size(), request_id);
    }

    /// 订阅指定交易所全市场期货五档（nCount=0）。exchange_id 如 "F2"(DCE),"F3"(SHFE),"F5"(INE) 等。
    int SubscribeMarket(const std::string &exchange_id, int request_id) {
        if (!api_) return -1;
        CHSNsqReqFutuDepthMarketDataField req{};
        copy_cstr(req.ExchangeID, sizeof(req.ExchangeID), exchange_id);
        return api_->ReqFutuDepthMarketDataSubscribe(&req, 0, request_id);
    }

private:
    CHSNsqApi *api_;
};

PYBIND11_MODULE(nsq_pybind, m) {
    m.doc() = "NSQ Market Data API Python Bindings (Linux only)";

    // --- 结构体绑定（常用字段） ---
    py::class_<CHSNsqRspInfoField>(m, "CHSNsqRspInfoField")
        .def_readonly("ErrorID", &CHSNsqRspInfoField::ErrorID)
        .def_property_readonly("ErrorMsg", [](const CHSNsqRspInfoField &f) { return std::string(f.ErrorMsg); });

    py::class_<CHSNsqReqUserLoginField>(m, "CHSNsqReqUserLoginField")
        .def(py::init<>())
        .def_property("AccountID",
            [](const CHSNsqReqUserLoginField &f) { return std::string(f.AccountID); },
            [](CHSNsqReqUserLoginField &f, const std::string &v) { copy_cstr(f.AccountID, sizeof(f.AccountID), v); })
        .def_property("Password",
            [](const CHSNsqReqUserLoginField &f) { return std::string(f.Password); },
            [](CHSNsqReqUserLoginField &f, const std::string &v) { copy_cstr(f.Password, sizeof(f.Password), v); });

    py::class_<CHSNsqRspUserLoginField>(m, "CHSNsqRspUserLoginField")
        .def_readonly("BranchID", &CHSNsqRspUserLoginField::BranchID)
        .def_property_readonly("AccountID", [](const CHSNsqRspUserLoginField &f) { return std::string(f.AccountID); })
        .def_property_readonly("UserName", [](const CHSNsqRspUserLoginField &f) { return std::string(f.UserName); })
        .def_readonly("TradingDay", &CHSNsqRspUserLoginField::TradingDay);

    py::class_<CHSNsqFutuDepthMarketDataField>(m, "CHSNsqFutuDepthMarketDataField")
        .def_readonly("TradingDay", &CHSNsqFutuDepthMarketDataField::TradingDay)
        .def_property_readonly("InstrumentID", [](const CHSNsqFutuDepthMarketDataField &f) { return std::string(f.InstrumentID); })
        .def_property_readonly("ExchangeID", [](const CHSNsqFutuDepthMarketDataField &f) { return std::string(f.ExchangeID); })
        .def_readonly("LastPrice", &CHSNsqFutuDepthMarketDataField::LastPrice)
        .def_readonly("PreSettlementPrice", &CHSNsqFutuDepthMarketDataField::PreSettlementPrice)
        .def_readonly("PreClosePrice", &CHSNsqFutuDepthMarketDataField::PreClosePrice)
        .def_readonly("OpenPrice", &CHSNsqFutuDepthMarketDataField::OpenPrice)
        .def_readonly("HighestPrice", &CHSNsqFutuDepthMarketDataField::HighestPrice)
        .def_readonly("LowestPrice", &CHSNsqFutuDepthMarketDataField::LowestPrice)
        .def_readonly("TradeVolume", &CHSNsqFutuDepthMarketDataField::TradeVolume)
        .def_readonly("OpenInterest", &CHSNsqFutuDepthMarketDataField::OpenInterest)
        .def_readonly("UpdateTime", &CHSNsqFutuDepthMarketDataField::UpdateTime)
        .def_readonly("ActionDay", &CHSNsqFutuDepthMarketDataField::ActionDay)
        .def_property_readonly("BidPrice", [](const CHSNsqFutuDepthMarketDataField &f) {
            std::vector<double> v;
            v.reserve(5);
            for (int i = 0; i < 5; i++) v.push_back(f.BidPrice[i]);
            return v;
        })
        .def_property_readonly("BidVolume", [](const CHSNsqFutuDepthMarketDataField &f) {
            std::vector<double> v;
            v.reserve(5);
            for (int i = 0; i < 5; i++) v.push_back(f.BidVolume[i]);
            return v;
        })
        .def_property_readonly("AskPrice", [](const CHSNsqFutuDepthMarketDataField &f) {
            std::vector<double> v;
            v.reserve(5);
            for (int i = 0; i < 5; i++) v.push_back(f.AskPrice[i]);
            return v;
        })
        .def_property_readonly("AskVolume", [](const CHSNsqFutuDepthMarketDataField &f) {
            std::vector<double> v;
            v.reserve(5);
            for (int i = 0; i < 5; i++) v.push_back(f.AskVolume[i]);
            return v;
        });

    // --- SPI 绑定（可在 Python 中继承并实现回调） ---
    py::class_<CHSNsqSpi, PyNsqSpi>(m, "CHSNsqSpi")
        .def(py::init<>())
        .def("OnFrontConnected", &CHSNsqSpi::OnFrontConnected)
        .def("OnFrontDisconnected", &CHSNsqSpi::OnFrontDisconnected)
        .def("OnRspUserLogin", &CHSNsqSpi::OnRspUserLogin)
        .def("OnRspFutuDepthMarketDataSubscribe", &CHSNsqSpi::OnRspFutuDepthMarketDataSubscribe)
        .def("OnRtnFutuDepthMarketData", &CHSNsqSpi::OnRtnFutuDepthMarketData);

    // --- API 绑定 ---
    py::class_<PyNsqApi>(m, "CHSNsqApi")
        .def(py::init<const std::string&, const std::string&>(), py::arg("flow_path") = "./log/", py::arg("sdk_cfg_file_path") = "")
        .def("RegisterSpi", &PyNsqApi::RegisterSpi)
        .def("RegisterFront", &PyNsqApi::RegisterFront)
        .def("Init", &PyNsqApi::Init, py::arg("lic_file"), py::arg("safe_level") = "", py::arg("pwd") = "", py::arg("ssl_file") = "", py::arg("ssl_pwd") = "")
        .def("ReqUserLogin", &PyNsqApi::ReqUserLogin)
        .def("ReqFutuDepthMarketDataSubscribe", &PyNsqApi::ReqFutuDepthMarketDataSubscribe, py::arg("contracts"), py::arg("request_id"))
        .def("SubscribeMarket", &PyNsqApi::SubscribeMarket, py::arg("exchange_id"), py::arg("request_id"))
        .def("GetApiErrorMsg", &PyNsqApi::GetApiErrorMsg)
        .def("GetApiVersion", &PyNsqApi::GetApiVersion);
}

