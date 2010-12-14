#!/usr/bin/env python
"""

SQLite implementation of GetParents

"""






from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.GetParents import GetParents as MySQLGetParents

class GetParents(MySQLGetParents):
    """

    SQLite implementation of GetParents

    """

    def GetUpdateGetParentsDialect(self):

        return 'SQLite'
