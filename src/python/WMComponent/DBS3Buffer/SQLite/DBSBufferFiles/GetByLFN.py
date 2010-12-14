#!/usr/bin/env python
"""

SQLite implementation of GetByLFN

"""






from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.GetByLFN import GetByLFN as MySQLGetByLFN

class GetByLFN(MySQLGetByLFN):
    """

    SQLite implementation of GetByLFN

    """

    def GetUpdateGetByLFNDialect(self):

        return 'SQLite'
