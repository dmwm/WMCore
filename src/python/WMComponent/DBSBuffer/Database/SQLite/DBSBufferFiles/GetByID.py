#!/usr/bin/env python
"""

SQLite implementation of GetByID

"""






from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.GetByID import GetByID as MySQLGetByID

class GetByID(MySQLGetByID):
    """

    SQLite implementation of GetByID

    """

    def GetUpdateGetByIDDialect(self):

        return 'SQLite'
