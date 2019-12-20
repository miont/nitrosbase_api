import os
import sys
from pynbase.dbapi import connect

NBASE_LIB_PATH = os.path.join('NBASE_LIB_PATH',
  '/home/kostrovskiy/work/data_storages/NitrosBase/releases/v_2.1/nitrosbaseuni_rc_2.1.36_linux_setup/nitrosbaseuni/bin/libnbclient.so')

conn = connect(lib_path=NBASE_LIB_PATH)
cursor = conn.cursor()
cursor.execute('SELECT * FROM car LIMIT 3')
for record in cursor:
  print(record)