#!/usr/bin/env python
"""
Oracle implementation of File.GetLocation
"""




#This has been modified for Oracle

from WMComponent.DBS3Buffer.MySQL.DBSBufferFiles.GetLocation import GetLocation as MySQLGetLocation

class GetLocation(MySQLGetLocation):
    sql = """select pnn from dbsbuffer_location
                where id in (select location from dbsbuffer_file_location
                    where filename in (select id from dbsbuffer_file where lfn=:lfn))"""
