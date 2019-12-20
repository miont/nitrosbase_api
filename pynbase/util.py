from .common import ApiType
from .cpp_api.api import CppApi
from .odbc.api import OdbcApi

def get_core_api(api_type:ApiType=ApiType.CPP_LIB):
  return {
    ApiType.CPP_LIB: CppApi,
    ApiType.ODBC:    OdbcApi
  }[api_type]
