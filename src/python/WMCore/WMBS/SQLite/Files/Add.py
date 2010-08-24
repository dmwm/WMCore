"""
SQLite implementation of AddFile
"""

from WMCore.WMBS.MySQL.Files.Add import Add as AddFileMySQL

class Add(AddFileMySQL):
    sql = AddFileMySQL.sql

