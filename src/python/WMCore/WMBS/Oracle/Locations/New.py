#!/usr/bin/env python
"""
_New_

Oracle implementation of Locations.New
"""

from WMCore.WMBS.MySQL.Locations.New import New as NewLocationMySQL

class New(NewLocationMySQL):    
    sql = """INSERT INTO wmbs_location (id, se_name) SELECT
             wmbs_location_SEQ.nextval, :location AS se_name FROM DUAL
             WHERE NOT EXISTS (SELECT se_name FROM wmbs_location
             WHERE se_name = :location)"""

              
