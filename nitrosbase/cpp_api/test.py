import os
import sys
import logging
import ctypes
import struct
from enum import IntEnum, auto

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)

NBASE_ROOT_PATH = '/home/kostrovskiy/work/data_storages/NitrosBase/releases/v_2.1/nitrosbaseuni_rc_2.1.36_linux_setup/nitrosbaseuni/'
NBASE_LIB_PATH = os.path.join(NBASE_ROOT_PATH, 'bin/libnbclient.so')

NB_HOST = "localhost"
NB_PORT = int(os.environ.get('NB_PORT', 3021))

QUERY1 = 'SELECT COUNT(*) FROM {table};'
QUERY2 = 'SELECT * FROM {table} LIMIT 3;'
TABLE_NAME = os.environ.get('TABLE_NAME','procurements')

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
####################################

class String(ctypes.Structure):
  _fields_ = [("str", ctypes.POINTER(ctypes.c_char)),
              ("len", ctypes.c_int32)]

class _NBValuesUnion(ctypes.Union):
  # _anonymous_ = ("str",)
  _fields_ = [("intv", ctypes.c_int32),
              ("int64v", ctypes.c_int64),
              ("dbl", ctypes.c_double),
              ("str", String)]

class NBValue(ctypes.Structure):
  _fields_ = [("type", ctypes.c_int),
              ("value", ctypes.c_byte*12),
              # ("str", ctypes.c_char_p),
              ("null", ctypes.c_bool)]
  @staticmethod
  def new():
    obj = NBValue()
    obj.value = _NBValuesUnion()
    return obj

def check_error(conn):
  logger.info('Check DB error for connection {}'.format(conn))
  err_type = lib.nb_errno(conn)
  logger.info('Error type: {}'.format(err_type))
  if err_type != NBErrorType.NB_OK:
    logger.error('{}: {}'.format(NBErrorType(err_type), lib.nb_err_text(conn).decode()))
  # logger.info('Error {}: {}'.format(lib.nb_errno(conn), lib.nb_err_text(conn)))

def _log_return(ret):
  logger.info('Return: {}'.format(ret))

def init_signatures(lib:ctypes.CDLL):

  lib.nb_errno.argtypes = [ctypes.c_void_p]
  lib.nb_errno.restype = ctypes.c_int
  lib.nb_err_text.argtypes = [ctypes.c_void_p]
  lib.nb_err_text.restype = ctypes.c_char_p

  lib.nb_fetch_row.argtypes = [ctypes.c_void_p]
  lib.nb_fetch_row.restype = ctypes.c_int
  lib.nb_field_count.argtypes = [ctypes.c_void_p]
  lib.nb_field_count.restype = ctypes.c_int
  lib.nb_field_name.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.POINTER(NBValue)]
  lib.nb_field_name.restype = ctypes.c_int
  lib.nb_field_type.argtypes = [ctypes.c_void_p, ctypes.c_int]
  lib.nb_field_type.restype = ctypes.c_int
  lib.nb_field_value.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.POINTER(NBValue)]
  lib.nb_field_value.restype = ctypes.c_int

def get_value(val: NBValue):
  if val.null:
    return "NULL"
  if val.type == NBDataType.NB_DATA_INT:
    return int.from_bytes(bytes(val.value)[:4], byteorder='little', signed=True)
  elif val.type == NBDataType.NB_DATA_INT64:
    return int.from_bytes(bytes(val.value)[:8], byteorder='little', signed=True)
  elif val.type == NBDataType.NB_DATA_DOUBLE:
    return struct.unpack('d', bytes(val.value)[:8])[0]
  elif val.type in (NBDataType.NB_DATA_STRING, NBDataType.NB_DATA_DATETIME, NBDataType.NB_DATA_DATE):
    addr = int.from_bytes(bytes(val.value)[:8], byteorder='little', signed=False)
    return ctypes.c_char_p(addr).value.decode()

# Load library
logger.info('Loading library {}'.format(NBASE_LIB_PATH))
lib = ctypes.cdll.LoadLibrary(NBASE_LIB_PATH)
init_signatures(lib)

try:
  logger.info('Connect DB')
  host = NB_HOST.encode()
  port = NB_PORT
  lib.nb_connect.argtypes = [ctypes.c_char_p, ctypes.c_int]
  lib.nb_connect.restype = ctypes.c_void_p
  conn = lib.nb_connect(host, port)
  logger.info('Connection: {}'.format(conn))
  lib.nb_errno.argtypes = [ctypes.c_void_p]
  # lib.nb_errno.restype = ctypes.c_int
  # err = lib.nb_errno(conn)
  # logger.info(f'Error type: {err}')
  # lib.nb_err_text.argtypes = [ctypes.c_void_p]
  # lib.nb_err_text.restype = ctypes.c_char_p
  # err_text = lib.nb_err_text(conn)
  # logger.info('Error text: {}'.format(err_text.decode()))
  if conn is None:
    check_error(conn)

  sql = QUERY2.format(table=TABLE_NAME).encode()
  logger.info('Execute SQL: {}'.format(sql.decode()))
  lib.nb_execute_sql.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t]
  lib.nb_execute_sql.restype = ctypes.c_int
  _log_return(lib.nb_execute_sql(conn, sql, len(sql)))
  check_error(conn)
  while lib.nb_fetch_row(conn) == NBErrorType.NB_OK:
    logger.info('Row fetched')
    field_count = lib.nb_field_count(conn)
    logger.info('Field count: {}'.format(field_count))
    for i in range(field_count):
      field_type = lib.nb_field_type(conn, i)
      logger.info(f'Field type: {NBDataType(field_type)}')
      field_name = NBValue()
      lib.nb_field_name(conn, i, ctypes.byref(field_name))
      logger.info('Field name struct data: 0x{}'.format(bytes(field_name).hex()))
      logger.info('Field name is null: {}'.format(field_name.null))
      logger.info('Field name type: {}'.format(field_name.type))
      field_name_len = int.from_bytes(bytes(field_name.value)[8:12], byteorder='little', signed=True)
      logger.info('Field name content: 0x{}'.format(bytes(field_name.value).hex()))

      logger.info('Field name address: 0x{}'.format(bytes(field_name.value)[:8].hex()))
      logger.info('Field name length: {}'.format(field_name_len))
      # logger.info('Field name bytes: {}'.format(bytes(field_name.value)[:8]))
      field_name_address = int.from_bytes(bytes(field_name.value)[:8], byteorder='little', signed=False)
      logger.info('Field name: {}'.format(ctypes.c_char_p(field_name_address).value.decode()))
      field_val = NBValue()
      lib.nb_field_value(conn, i, ctypes.byref(field_val))
      # logger.info('Field value: {}'.format(get_value(field_val)))
      logger.info('Field value is null: {}'.format(field_val.null))
      logger.info('Field value content: 0x{}'.format(bytes(field_val.value).hex()))
      logger.info('Field value: {}'.format(get_value(field_val)))
      # if field_val.type != NBDataType.NB_DATA_STRING:
      #   logger.info('Field value bytes: 0x{}'.format(bytes(field_val.value).hex()))
      #   logger.info('Field value: {}'.format(int.from_bytes(bytes(field_val.value)[:4], byteorder='little', signed=True)))

finally:
  logger.info('Closing connection')
  lib.nb_disconnect.argtypes = [ctypes.c_void_p]
  lib.nb_disconnect.restype = ctypes.c_int
  _log_return(lib.nb_disconnect(conn))