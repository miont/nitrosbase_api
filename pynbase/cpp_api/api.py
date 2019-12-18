import os
import sys
import logging
import struct
import ctypes

from ..common import NBDataType, NBErrorType, InterfaceError, DatabaseError

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)

class NBValue(ctypes.Structure):
  _fields_ = [("type", ctypes.c_int),
              ("value", ctypes.c_byte*12),
              ("null", ctypes.c_bool)]

def _init_signatures(lib:ctypes.CDLL):

  lib.nb_connect.argtypes = [ctypes.c_char_p, ctypes.c_int]
  lib.nb_connect.restype = ctypes.c_void_p
  lib.nb_disconnect.argtypes = [ctypes.c_void_p]
  lib.nb_disconnect.restype = ctypes.c_int

  lib.nb_errno.argtypes = [ctypes.c_void_p]
  lib.nb_errno.restype = ctypes.c_int
  lib.nb_err_text.argtypes = [ctypes.c_void_p]
  lib.nb_err_text.restype = ctypes.c_char_p

  lib.nb_execute_sql.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t]
  lib.nb_execute_sql.restype = ctypes.c_int
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

def _get_value(val: NBValue):
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

class CppApi(object):
  def __init__(self, lib_path:str=None):
    if lib_path is None:
      raise InterfaceError('Parameter lib_path (NitrosBase interface library path) must be provided for C++ API')
    logger.info('Loading NitrosBase library {}'.format(lib_path))
    self.lib = self._load_library(lib_path)
    
  def _load_library(self, lib_path):
    lib = ctypes.cdll.LoadLibrary(lib_path)
    if lib is None:
      raise InterfaceError('Library not found on path {}'.format(lib_path))
    _init_signatures(lib)
    return lib

  def connect(self, host:str, port:int, user:str=None, password:str=None):
    self.conn = self.lib.nb_connect(host.encode(), port)
    return self.conn is not None
  
  def disconnect(self):
    if self.conn is not None:
      self.lib.nb_disconnect(self.conn)

  def read_record(self):
    return self.lib.nb_fetch_row(self.conn) == NBErrorType.NB_OK

  def execute_sql(self, query:str):
    self.lib.nb_execute_sql(self.conn, query.encode(), len(query))
    self.check_error()

  def field_count(self):
    return self.lib.nb_field_count(self.conn)

  def field_name(self, idx):
    field_name = NBValue()
    self.lib.nb_field_name(self.conn, idx, ctypes.byref(field_name))
    return _get_value(field_name)

  def field_value(self, idx):
    field_val = NBValue()
    self.lib.nb_field_value(self.conn, idx, ctypes.byref(field_val))
    return _get_value(field_val)

  def check_error(self):
    err_type = self.lib.nb_errno(self.conn)
    if err_type != NBErrorType.NB_OK:
      err = self.lib.nb_err_text(self.conn).decode()
      logger.error('{}: {}'.format(NBErrorType(err_type), err))
      raise DatabaseError(err)