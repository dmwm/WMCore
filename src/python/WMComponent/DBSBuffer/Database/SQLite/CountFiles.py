#!/usr/bin/env python
"""
_CountFiles_

SQLite implementation of DBSBuffer.CountFiles
"""

__revision__ = "$Id: CountFiles.py,v 1.1 2009/10/13 19:55:55 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.CountFiles import CountFiles as MySQLCountFiles

class CountFiles(MySQLCountFiles):
    """
    _CountFiles_

    """
    pass
