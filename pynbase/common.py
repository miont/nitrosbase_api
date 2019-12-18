from enum import Enum, IntEnum, auto

############# Constants ###########
# Errors
class NBErrorType(IntEnum):
  NB_OK = 0
  NB_NO_DATA = auto()

  NB_ERROR_FILE = 100
  NB_ERROR_MEM = auto()
  NB_ERROR_ARGS = auto()
  NB_ERROR_CPPFUNC = auto()
  NB_ERROR_QUERY = auto()
  NB_ERROR_CONNECT = auto()
  NB_ERROR_ANOTHER = 200

class NBDataType(IntEnum):
  NB_DATA_NONE = 0
  # database field types
  NB_DATA_STRING = auto()
  NB_DATA_INT = auto()
  NB_DATA_INT64 = auto()
  NB_DATA_DOUBLE = auto()
  NB_DATA_DATETIME = auto()
  NB_DATA_BOOL = auto()
  NB_DATA_DATE = auto()
  NB_DATA_URI = auto()

class ApiType(Enum):
  CPP_LIB = auto()
  ODBC = auto()
####################################

############# Exceptions ###########
class Warning(Exception):
  pass

class Error(Exception):
  pass

class InterfaceError(Error):
  pass

class DatabaseError(Error):
  pass

class NotSupportedError(Error):
  pass
####################################