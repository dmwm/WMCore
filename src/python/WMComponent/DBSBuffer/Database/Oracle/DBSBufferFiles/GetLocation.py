#!/usr/bin/env python
"""
Oracle implementation of File.GetLocation
"""

__revision__ = "$Id: GetLocation.py,v 1.1 2009/05/15 16:47:41 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

#This has been modified for Oracle

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.GetLocation import GetLocation as MySQLGetLocation

class GetLocation(MySQLGetLocation):
    sql = """select se_name from dbsbuffer_location 
                where id in (select location from dbsbuffer_file_location 
                    where "file" in (select id from dbsbuffer_file where lfn=:lfn))"""
                    
    

