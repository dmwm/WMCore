#!/usr/bin/env python
"""
_Exists_

SQLite implementation of BossLite.Job.Exists
"""

__all__ = []
__revision__ = "$Id: Exists.py,v 1.1 2010/03/30 10:15:08 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.BossLite.MySQL.Job.Exists import Exists as MySQLExists

class Exists(MySQLExists):
    """
    Identical to MySQL

    """
