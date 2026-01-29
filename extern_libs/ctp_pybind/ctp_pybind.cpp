#include <pybind11/pybind11.h>
#include <pybind11/functional.h>
#include <pybind11/stl.h>
#include "ThostFtdcMdApi.h"
#include <string>
#include <vector>
#include <iostream>

namespace py = pybind11;

// --- SPI 包装类，用于处理回调并转发给 Python ---
class PyMdSpi : public CThostFtdcMdSpi {
public:
    using CThostFtdcMdSpi::CThostFtdcMdSpi;

    void OnFrontConnected() override {
        PYBIND11_OVERLOAD(void, CThostFtdcMdSpi, OnFrontConnected);
    }

    void OnFrontDisconnected(int nReason) override {
        PYBIND11_OVERLOAD(void, CThostFtdcMdSpi, OnFrontDisconnected, nReason);
    }

    void OnRspUserLogin(CThostFtdcRspUserLoginField *pRspUserLogin, CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast) override {
        PYBIND11_OVERLOAD(void, CThostFtdcMdSpi, OnRspUserLogin, pRspUserLogin, pRspInfo, nRequestID, bIsLast);
    }

    void OnRtnDepthMarketData(CThostFtdcDepthMarketDataField *pDepthMarketData) override {
        PYBIND11_OVERLOAD(void, CThostFtdcMdSpi, OnRtnDepthMarketData, pDepthMarketData);
    }

    void OnRspSubMarketData(CThostFtdcSpecificInstrumentField *pSpecificInstrument, CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast) override {
        PYBIND11_OVERLOAD(void, CThostFtdcMdSpi, OnRspSubMarketData, pSpecificInstrument, pRspInfo, nRequestID, bIsLast);
    }

    void OnRspError(CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast) override {
        PYBIND11_OVERLOAD(void, CThostFtdcMdSpi, OnRspError, pRspInfo, nRequestID, bIsLast);
    }
};

// --- API 包装类 ---
class PyMdApi {
public:
    PyMdApi(const std::string &flow_path = "") {
        api = CThostFtdcMdApi::CreateFtdcMdApi(flow_path.c_str());
    }

    ~PyMdApi() {
        if (api) {
            api->Release();
            api = nullptr;
        }
    }

    void RegisterSpi(PyMdSpi *spi) {
        if (api) api->RegisterSpi(spi);
    }

    void RegisterFront(char *pszFrontAddress) {
        if (api) api->RegisterFront(pszFrontAddress);
    }

    void Init() {
        if (api) api->Init();
    }

    int Join() {
        return api ? api->Join() : -1;
    }

    int ReqUserLogin(CThostFtdcReqUserLoginField *pReqUserLoginField, int nRequestID) {
        return api ? api->ReqUserLogin(pReqUserLoginField, nRequestID) : -1;
    }

    int SubscribeMarketData(std::vector<std::string> symbols) {
        if (!api) return -1;
        std::vector<char*> p_symbols;
        for (auto &s : symbols) {
            p_symbols.push_back(const_cast<char*>(s.c_str()));
        }
        return api->SubscribeMarketData(p_symbols.data(), p_symbols.size());
    }

    const char* GetApiVersion() {
        return CThostFtdcMdApi::GetApiVersion();
    }

private:
    CThostFtdcMdApi *api;
};

PYBIND11_MODULE(ctp_pybind, m) {
    m.doc() = "CTP Market Data API Python Bindings";

    // --- 结构体绑定 ---
    py::class_<CThostFtdcRspInfoField>(m, "CThostFtdcRspInfoField")
        .def_readonly("ErrorID", &CThostFtdcRspInfoField::ErrorID)
        .def_property_readonly("ErrorMsg", [](const CThostFtdcRspInfoField &f) {
            return std::string(f.ErrorMsg);
        });

    py::class_<CThostFtdcRspUserLoginField>(m, "CThostFtdcRspUserLoginField")
        .def_property_readonly("TradingDay", [](const CThostFtdcRspUserLoginField &f) { return std::string(f.TradingDay); })
        .def_property_readonly("LoginTime", [](const CThostFtdcRspUserLoginField &f) { return std::string(f.LoginTime); })
        .def_property_readonly("BrokerID", [](const CThostFtdcRspUserLoginField &f) { return std::string(f.BrokerID); })
        .def_property_readonly("UserID", [](const CThostFtdcRspUserLoginField &f) { return std::string(f.UserID); })
        .def_readonly("FrontID", &CThostFtdcRspUserLoginField::FrontID)
        .def_readonly("SessionID", &CThostFtdcRspUserLoginField::SessionID);

    py::class_<CThostFtdcReqUserLoginField>(m, "CThostFtdcReqUserLoginField")
        .def(py::init<>())
        .def_property("BrokerID", 
            [](const CThostFtdcReqUserLoginField &f) { return std::string(f.BrokerID); },
            [](CThostFtdcReqUserLoginField &f, const std::string &v) { strncpy(f.BrokerID, v.c_str(), sizeof(f.BrokerID)); })
        .def_property("UserID", 
            [](const CThostFtdcReqUserLoginField &f) { return std::string(f.UserID); },
            [](CThostFtdcReqUserLoginField &f, const std::string &v) { strncpy(f.UserID, v.c_str(), sizeof(f.UserID)); })
        .def_property("Password", 
            [](const CThostFtdcReqUserLoginField &f) { return std::string(f.Password); },
            [](CThostFtdcReqUserLoginField &f, const std::string &v) { strncpy(f.Password, v.c_str(), sizeof(f.Password)); });

    py::class_<CThostFtdcDepthMarketDataField>(m, "CThostFtdcDepthMarketDataField")
        .def_property_readonly("TradingDay", [](const CThostFtdcDepthMarketDataField &f) { return std::string(f.TradingDay); })
        .def_property_readonly("InstrumentID", [](const CThostFtdcDepthMarketDataField &f) { return std::string(f.InstrumentID); })
        .def_property_readonly("ExchangeID", [](const CThostFtdcDepthMarketDataField &f) { return std::string(f.ExchangeID); })
        .def_readonly("LastPrice", &CThostFtdcDepthMarketDataField::LastPrice)
        .def_readonly("PreSettlementPrice", &CThostFtdcDepthMarketDataField::PreSettlementPrice)
        .def_readonly("PreClosePrice", &CThostFtdcDepthMarketDataField::PreClosePrice)
        .def_readonly("PreOpenInterest", &CThostFtdcDepthMarketDataField::PreOpenInterest)
        .def_readonly("OpenPrice", &CThostFtdcDepthMarketDataField::OpenPrice)
        .def_readonly("HighestPrice", &CThostFtdcDepthMarketDataField::HighestPrice)
        .def_readonly("LowestPrice", &CThostFtdcDepthMarketDataField::LowestPrice)
        .def_readonly("Volume", &CThostFtdcDepthMarketDataField::Volume)
        .def_readonly("Turnover", &CThostFtdcDepthMarketDataField::Turnover)
        .def_readonly("OpenInterest", &CThostFtdcDepthMarketDataField::OpenInterest)
        .def_readonly("ClosePrice", &CThostFtdcDepthMarketDataField::ClosePrice)
        .def_readonly("SettlementPrice", &CThostFtdcDepthMarketDataField::SettlementPrice)
        .def_readonly("UpperLimitPrice", &CThostFtdcDepthMarketDataField::UpperLimitPrice)
        .def_readonly("LowerLimitPrice", &CThostFtdcDepthMarketDataField::LowerLimitPrice)
        .def_property_readonly("UpdateTime", [](const CThostFtdcDepthMarketDataField &f) { return std::string(f.UpdateTime); })
        .def_readonly("UpdateMillisec", &CThostFtdcDepthMarketDataField::UpdateMillisec)
        .def_readonly("BidPrice1", &CThostFtdcDepthMarketDataField::BidPrice1)
        .def_readonly("BidVolume1", &CThostFtdcDepthMarketDataField::BidVolume1)
        .def_readonly("AskPrice1", &CThostFtdcDepthMarketDataField::AskPrice1)
        .def_readonly("AskVolume1", &CThostFtdcDepthMarketDataField::AskVolume1)
        .def_readonly("AveragePrice", &CThostFtdcDepthMarketDataField::AveragePrice)
        .def_property_readonly("ActionDay", [](const CThostFtdcDepthMarketDataField &f) { return std::string(f.ActionDay); });

    py::class_<CThostFtdcSpecificInstrumentField>(m, "CThostFtdcSpecificInstrumentField")
        .def_property_readonly("InstrumentID", [](const CThostFtdcSpecificInstrumentField &f) { return std::string(f.InstrumentID); });

    // --- SPI 绑定 ---
    py::class_<CThostFtdcMdSpi, PyMdSpi>(m, "CThostFtdcMdSpi")
        .def(py::init<>())
        .def("OnFrontConnected", &CThostFtdcMdSpi::OnFrontConnected)
        .def("OnFrontDisconnected", &CThostFtdcMdSpi::OnFrontDisconnected)
        .def("OnRspUserLogin", &CThostFtdcMdSpi::OnRspUserLogin)
        .def("OnRtnDepthMarketData", &CThostFtdcMdSpi::OnRtnDepthMarketData)
        .def("OnRspSubMarketData", &CThostFtdcMdSpi::OnRspSubMarketData)
        .def("OnRspError", &CThostFtdcMdSpi::OnRspError);

    // --- API 绑定 ---
    py::class_<PyMdApi>(m, "CThostFtdcMdApi")
        .def(py::init<const std::string &>(), py::arg("flow_path") = "")
        .def("RegisterSpi", &PyMdApi::RegisterSpi)
        .def("RegisterFront", &PyMdApi::RegisterFront)
        .def("Init", &PyMdApi::Init)
        .def("ReqUserLogin", &PyMdApi::ReqUserLogin)
        .def("SubscribeMarketData", &PyMdApi::SubscribeMarketData)
        .def("GetApiVersion", &PyMdApi::GetApiVersion);
}
