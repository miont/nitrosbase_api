import sys
import logging

import nbase.common as common
from nbase.util import get_core_api

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)

def connect(host:str='localhost', port:int=3020, 
            user:str=None, password:str=None, 
            api:common.ApiType=common.ApiType.CPP_LIB, **params):
  logger.info('Connecting to DB on {}:{}'.format(host, port))
  return Connection(host, port, user, password, core_api_cls=get_core_api(), **params)

class Connection(object):
  def __init__(self, host, port, user, password, core_api_cls, **params):
    self.db = core_api_cls(**params)
    if not self.db.connect(host, port, user, password):
      raise DatabaseError('Cannot connect to DB with URI {host}:{port}'
        .format(host=host, port=port))

  def close(self):
    self.db.disconnect()
    self.db = None
  
  def commit(self):
    raise common.NotSupportedError()

  def rollback(self):
    raise common.NotSupportedError()
  
  def cursor(self):
    if self.db is None:
      raise common.Error('Connection is closed')
    return Cursor(db=self.db)

class Cursor(object):
  def __init__(self, db):
    self.arraysize = 1
    self.db = db

  @property
  def description(self):
    return None

  @property
  def rowcount(self):
    return -1

  def close(self):
    self.db = None

  def execute(self, query:str):
    """Execute query (SQL in current implementation)
    Args:
      query (str): Query
    """
    if self.db is None:
      raise common.Error('Cursor is closed')
    self.db.execute_sql(query)

  def executemany(self, query, params):
    raise NotImplementedError()

  def fetchone(self):
    """Fetch next row (record) from result set
    """
    if self.db is None:
      raise common.Error('Cursor is closed')
    if self.db.read_record():
      data = {self.db.field_name(i): self.db.field_value(i) 
              for i in range(self.db.field_count())}
      return data
    else:
      return None

  def fetchmany(self):
    raise NotImplementedError()

  # Iterator protocol implemetation
  def __iter__(self):
    return self

  def __next__(self):
    data = self.fetchone()
    if data is None:
      raise StopIteration
    else:
      return data