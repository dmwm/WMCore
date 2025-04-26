#!/usr/bin/env python
"""
_AddLocation_

Oracle implementation of DBSBufferFiles.AddLocation
"""

from WMComponent.DBS3Buffer.MySQL.DBSBufferFiles.AddLocation import AddLocation as MySQLAddLocation

class AddLocation(MySQLAddLocation):

    sql = """INSERT INTO dbsbuffer_location (pnn)
             SELECT :location FROM dual
             WHERE NOT EXISTS (SELECT id FROM dbsbuffer_location WHERE pnn = :location)"""
