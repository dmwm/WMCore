#!/usr/bin/env python
"""

SQLite implementation of GetParents

"""

__revision__ = "$Id: GetParents.py,v 1.1 2009/05/14 16:21:51 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"


from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.GetParents import GetParents as MySQLGetParents

class GetParents(MySQLGetParents):
    """

    SQLite implementation of GetParents

    """

    def GetUpdateGetParentsDialect(self):

        return 'SQLite'
