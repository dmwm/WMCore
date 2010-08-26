#!/usr/bin/env python
"""
_GetJobs_

SQLite implementation of BossLite.Task.GetJobs
"""

__all__ = []
__revision__ = "$Id: GetJobs.py,v 1.1 2010/03/30 10:20:49 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.BossLite.MySQL.Task.GetJobs import GetJobs as MySQLGetJobs

class GetJobs(MySQLGetJobs):
    """
    Identical to MySQL

    """
