#!/usr/bin/env python
"""

SQLite implementation of GetRunLumiFile

"""

__revision__ = "$Id: GetRunLumiFile.py,v 1.1 2009/05/14 16:21:51 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"


from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.GetRunLumiFile import GetRunLumiFile as MySQLGetRunLumiFile

class GetRunLumiFile(MySQLGetRunLumiFile):
    """

    SQLite implementation of GetRunLumiFile

    """

    def GetUpdateGetRunLumiFileDialect(self):

        return 'SQLite'
