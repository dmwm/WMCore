"""
SQLite implementation of AddLocation
"""

from WMCore.WMBS.MySQL.Locations.New import New as NewLocationMySQL

class New(NewLocationMySQL):
    sql = NewLocationMySQL.sql