#!/usr/bin/env python
"""
_SetFWJRPath_

SQLite implementation of Jobs.SetFWJRPath
"""

__revision__ = "$Id: SetFWJRPath.py,v 1.1 2009/10/13 20:04:11 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.SetFWJRPath import SetFWJRPath as MySQLSetFWJRPath

class SetFWJRPath(MySQLSetFWJRPath):
    """
    Identical to MySQL version.
    """
    pass
