#!/usr/bin/env python
"""

SQLite implementation of Heritage

"""

__revision__ = "$Id: Heritage.py,v 1.1 2009/05/14 16:21:51 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"


from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.Heritage import Heritage as MySQLHeritage

class Heritage(MySQLHeritage):
    """

    SQLite implementation of Heritage

    """

    def GetUpdateHeritageDialect(self):

        return 'SQLite'
