#!/usr/bin/env python
"""
_New_

SQLite implementation of BossLite.RunningJob.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.1 2010/03/30 10:23:30 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.BossLite.MySQL.RunningJob.New import New as MySQLNew

class New(MySQLNew):
    """
    Identical to MySQL

    """
