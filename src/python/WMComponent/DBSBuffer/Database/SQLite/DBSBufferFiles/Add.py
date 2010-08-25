#!/usr/bin/env python
"""

SQLite implementation of AddFile

"""

__revision__ = "$Id: Add.py,v 1.1 2009/05/14 16:21:51 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"



from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.Add import Add as MySQLAdd

class Add(MySQLAdd):
    """

    SQLite implementation of AddFile

    """

    def GetUpdateAddDialect(self):

        return 'SQLite'
