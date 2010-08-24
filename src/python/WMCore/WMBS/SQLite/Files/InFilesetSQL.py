"""
SQLite implementation of Files.InFileset
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Files.InFilesetSQL import InFileset as InFilesetMySQL

class InFileset(InFilesetMySQL, SQLiteBase):
    sql = InFilesetMySQL.sql