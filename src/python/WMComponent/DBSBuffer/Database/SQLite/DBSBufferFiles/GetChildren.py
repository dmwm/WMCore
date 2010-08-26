#!/usr/bin/env python
"""
_GetChildren_

SQLite implementation of DBSBufferFile.GetChildren
"""

__revision__ = "$Id: GetChildren.py,v 1.1 2010/01/13 19:54:44 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.GetChildren import GetChildren as MySQLGetChildren

class GetChildren(MySQLGetChildren):
    pass
