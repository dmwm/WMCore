#!/usr/bin/env python
"""

SQLite implementation of GetByLFN

"""

__revision__ = "$Id: GetByLFN.py,v 1.1 2009/05/14 16:21:51 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"


from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.GetByLFN import GetByLFN as MySQLGetByLFN

class GetByLFN(MySQLGetByLFN):
    """

    SQLite implementation of GetByLFN

    """

    def GetUpdateGetByLFNDialect(self):

        return 'SQLite'
