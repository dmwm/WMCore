#!/usr/bin/env python
"""
_Load_

SQLite implementation of BossLite.RunningJob.Load
"""

__all__ = []
__revision__ = "$Id: Load.py,v 1.1 2010/05/09 20:07:57 spigafi Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.BossLite.MySQL.RunningJob.Load import Load as MySQLLoad

class Load(MySQLLoad):
    """
    Identical to MySQL

    """
