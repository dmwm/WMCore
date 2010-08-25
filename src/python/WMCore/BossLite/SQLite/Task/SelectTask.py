#!/usr/bin/env python
"""
_SelectTask_

SQLite implementation of BossLite.Task.SelectTask
"""

__all__ = []
__revision__ = "$Id: SelectTask.py,v 1.1 2010/04/09 19:43:10 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.BossLite.MySQL.Task.SelectTask import SelectTask as MySQLSelectTask

class SelectTask(MySQLSelectTask):
    """
    Same as MySQL

    """
