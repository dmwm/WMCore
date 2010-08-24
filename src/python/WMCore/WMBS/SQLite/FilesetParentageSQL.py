"""
MySQL implementation of FilesetParentage
"""
from WMCore.WMBS.MySQL.Base import MySQLBase

from WMCore.WMBS.MySQL.FilesetParentageSQL import FilesetParentage as FilesetParentageMySQL

class FilesetParentage(FilesetParentageMySQL, SQLiteBase):
    sql = FilesetParentageMySQL.sql