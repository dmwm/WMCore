"""
SQLite implementation of SetFileLocation
"""

from WMCore.WMBS.MySQL.Files.SetLocation import SetLocation as SetLocationMySQL
from sets import Set

class SetLocation(SetLocationMySQL):
    sql = SetLocationMySQL.sql
        
