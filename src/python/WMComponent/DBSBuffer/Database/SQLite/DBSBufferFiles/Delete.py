#!/usr/bin/env python
"""

SQLite implementation of Delete

"""






from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.Delete import Delete as MySQLDelete

class Delete(MySQLDelete):
    """

    SQLite implementation of Delete

    """

    def GetUpdateDeleteDialect(self):

        return 'SQLite'
