#!/usr/bin/env python
"""
_New_

SQLite implementation of Locations.New
"""

from WMCore.WMBS.MySQL.Locations.New import New as NewLocationMySQL

class New(NewLocationMySQL):
    sql = """INSERT INTO wmbs_location (se_name) SELECT :location AS se_name
             WHERE NOT EXISTS (SELECT se_name FROM wmbs_location WHERE
             se_name = :location)"""
