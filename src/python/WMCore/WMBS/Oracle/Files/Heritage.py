"""
SQLite implementation of Files.Heritage
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Files.Heritage import Heritage as HeritageMySQL

class Heritage(HeritageMySQL, SQLiteBase):
    sql = HeritageMySQL.sql