#!/usr/bin/env python
"""
_AddLocation_

Oracle implementation of DBSBufferFiles.AddLocation
"""

__revision__ = "$Id: AddLocation.py,v 1.4 2009/10/22 14:47:30 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.AddLocation import AddLocation as \
     MySQLAddLocation

class AddLocation(MySQLAddLocation):
    existsSQL = """SELECT se_name, id FROM dbsbuffer_location
                         WHERE se_name = :location"""

    sql = """INSERT INTO dbsbuffer_location (se_name) 
               SELECT :location AS se_name FROM DUAL WHERE NOT EXISTS
                (SELECT se_name FROM dbsbuffer_location WHERE se_name = :location)"""
    
