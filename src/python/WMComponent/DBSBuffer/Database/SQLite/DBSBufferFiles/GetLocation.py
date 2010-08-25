#!/usr/bin/env python
"""

SQLite implementation of GetLocation

"""






from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.GetLocation import GetLocation as MySQLGetLocation

class GetLocation(MySQLGetLocation):
    """

    SQLite implementation of GetLocation

    """

    def GetUpdateGetLocationDialect(self):

        return 'SQLite'
