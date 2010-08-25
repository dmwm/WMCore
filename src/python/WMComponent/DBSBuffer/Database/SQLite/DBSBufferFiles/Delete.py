#!/usr/bin/env python
"""

SQLite implementation of Delete

"""

__revision__ = "$Id: Delete.py,v 1.1 2009/05/14 16:21:51 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"


from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.Delete import Delete as MySQLDelete

class Delete(MySQLDelete):
    """

    SQLite implementation of Delete

    """

    def GetUpdateDeleteDialect(self):

        return 'SQLite'
