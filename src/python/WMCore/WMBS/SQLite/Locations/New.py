#!/usr/bin/env python
"""
_New_

SQLite implementation of Locations.New
"""

from WMCore.WMBS.MySQL.Locations.New import New as NewLocationMySQL

class New(NewLocationMySQL):
    sql = """INSERT INTO wmbs_location (site_name, se_name, ce_name, job_slots) 
               SELECT :location AS site_name, :sename AS se_name,
                      :cename AS ce_name, :slots AS job_slots
                      WHERE NOT EXISTS
                (SELECT * FROM wmbs_location WHERE site_name = :location)"""

