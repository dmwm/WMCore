#!/usr/bin/env python
"""

SQLite implementation of GetRunLumiFile

"""






from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.GetRunLumiFile import GetRunLumiFile as MySQLGetRunLumiFile

class GetRunLumiFile(MySQLGetRunLumiFile):
    """

    SQLite implementation of GetRunLumiFile

    """

    def GetUpdateGetRunLumiFileDialect(self):

        return 'SQLite'
