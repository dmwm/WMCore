#!/usr/bin/env python
"""
Oracle implementation of SetLocation
"""

#Modified for Oracle

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.SetLocation import SetLocation as MySQLSetLocation

__revision__ = "$Id: SetLocation.py,v 1.1 2009/05/15 16:47:41 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

class SetLocation(MySQLSetLocation):
    sql = """insert into dbsbuffer_file_location ("file", location) 
             select dbsbuffer_file.id, dbsbuffer_location.id from dbsbuffer_file, dbsbuffer_location 
             where dbsbuffer_file.lfn = :lfn
             and dbsbuffer_location.se_name = :location"""
                
