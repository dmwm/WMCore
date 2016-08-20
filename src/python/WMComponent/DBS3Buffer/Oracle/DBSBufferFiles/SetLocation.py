#!/usr/bin/env python
"""
_SetLocation_

Oracle implementation of DBSBufferFiles.SetLocation
"""




from WMComponent.DBS3Buffer.MySQL.DBSBufferFiles.SetLocation import SetLocation as \
     MySQLSetLocation

class SetLocation(MySQLSetLocation):
    """
    Insert unique lfn, location row
    """
    sql = """INSERT INTO dbsbuffer_file_location (filename, location)
               SELECT :fileid, :locationid FROM DUAL WHERE NOT EXISTS
                 (SELECT filename FROM dbsbuffer_file_location
                 WHERE filename= :fileid and location= :locationid)
               """
