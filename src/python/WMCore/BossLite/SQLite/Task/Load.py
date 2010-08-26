#!/usr/bin/env python
"""
_Load_

SQLite implementation of BossLite.Task.Load
"""

__all__ = []
__revision__ = "$Id: Load.py,v 1.1 2010/05/21 12:04:29 spigafi Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.BossLite.MySQL.Task.Load import Load as MySQLLoad

class Load(MySQLLoad):
    """
    Same as MySQL

    """
