#!/usr/bin/env python
"""

SQLite implementation of AddFile

"""

__revision__ = "$Id: Add.py,v 1.2 2009/07/13 19:35:41 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "mnorman@fnal.gov"



from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.Add import Add as MySQLAdd

class Add(MySQLAdd):
    """
    SQLite implementation of AddFile
    """
    pass
