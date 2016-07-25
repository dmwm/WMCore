#!/usr/bin/env python
"""
_AddLocation_

Oracle implementation of DBSBufferFiles.AddLocation
"""

from WMComponent.DBS3Buffer.MySQL.DBSBufferFiles.AddLocation import AddLocation as MySQLAddLocation

class AddLocation(MySQLAddLocation):

    sql = """INSERT INTO dbsbuffer_location
             (id, pnn)
             SELECT dbsbuffer_location_seq.nextval, :location
             FROM DUAL
             WHERE NOT EXISTS
               ( SELECT *
                 FROM dbsbuffer_location
                 WHERE pnn = :location )
             """
