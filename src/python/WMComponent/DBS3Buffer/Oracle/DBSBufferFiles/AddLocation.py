#!/usr/bin/env python
"""
_AddLocation_

Oracle implementation of DBSBufferFiles.AddLocation
"""

from WMCore.Database.DBFormatter import DBFormatter

from WMComponent.DBS3Buffer.MySQL.DBSBufferFiles.AddLocation import AddLocation as \
     MySQLAddLocation

class AddLocation(MySQLAddLocation):
    sql = """INSERT INTO dbsbuffer_location (se_name)
               SELECT :location AS se_name FROM DUAL WHERE NOT EXISTS
                (SELECT se_name FROM dbsbuffer_location WHERE se_name = :location)"""
