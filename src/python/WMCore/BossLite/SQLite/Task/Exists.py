#!/usr/bin/env python
"""
_Exists_

SQLite implementation of BossLite.Task.Exists
"""

__all__ = []
__revision__ = "$Id: Exists.py,v 1.1 2010/03/30 10:19:01 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.BossLite.MySQL.Task.Exists import Exists as MySQLExists

class Exists(MySQLExists):
    """
    Identical to MySQL

    """
