from nbase.common import ApiType
import nbase.cpp_api.api as cpp_api
import nbase.odbc.api as odbc

def get_core_api(api_type:ApiType=ApiType.CPP_LIB):
  return {
    ApiType.CPP_LIB: cpp_api.CppApi,
    ApiType.ODBC:    odbc.OdbcApi
  }[api_type]
