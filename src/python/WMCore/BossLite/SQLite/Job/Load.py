#!/usr/bin/env python
"""
_Load_

SQLite implementation of BossLite.Job.Load
"""

__all__ = []
__revision__ = "$Id: Load.py,v 1.1 2010/05/21 12:05:57 spigafi Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.BossLite.MySQL.Job.Load import Load as MySQLLoad

class Load(MySQLLoad):
    """
    Same as MySQL

    """
