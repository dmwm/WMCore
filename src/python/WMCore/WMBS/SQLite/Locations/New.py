#!/usr/bin/env python
"""
_New_

SQLite implementation of Locations.New
"""

from WMCore.WMBS.MySQL.Locations.New import New as NewLocationMySQL

class New(NewLocationMySQL):
    sql = """INSERT INTO wmbs_location (site_name) SELECT :location AS site_name
             WHERE NOT EXISTS (SELECT site_name FROM wmbs_location WHERE
             site_name = :location)"""
