#!/usr/bin/env python
"""
Oracle implementation of File.GetLocation
"""

__revision__ = "$Id: GetLocation.py,v 1.2 2009/05/18 20:14:03 mnorman Exp $"
__version__ = "$Revision: 1.2 $"

#This has been modified for Oracle

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.GetLocation import GetLocation as MySQLGetLocation

class GetLocation(MySQLGetLocation):
    sql = """select se_name from dbsbuffer_location 
                where id in (select location from dbsbuffer_file_location 
                    where filename in (select id from dbsbuffer_file where lfn=:lfn))"""
                    
    

