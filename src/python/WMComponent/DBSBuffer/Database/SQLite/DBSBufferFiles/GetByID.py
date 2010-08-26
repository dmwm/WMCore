#!/usr/bin/env python
"""

SQLite implementation of GetByID

"""

__revision__ = "$Id: GetByID.py,v 1.1 2009/05/14 16:21:51 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"


from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.GetByID import GetByID as MySQLGetByID

class GetByID(MySQLGetByID):
    """

    SQLite implementation of GetByID

    """

    def GetUpdateGetByIDDialect(self):

        return 'SQLite'
