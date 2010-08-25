#!/usr/bin/env python
"""
_SelectJob_

SQLite implementation of BossLite.Job.SelectJob
"""

__all__ = []
__revision__ = "$Id: SelectJob.py,v 1.1 2010/04/09 19:49:09 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.BossLite.MySQL.Job.SelectJob import SelectJob as MySQLSelectJob

class SelectJob(MySQLSelectJob):
    """
    Same as MySQL

    """
