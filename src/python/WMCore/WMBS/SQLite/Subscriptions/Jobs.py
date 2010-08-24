#!/usr/bin/env python
"""
_Jobs_

SQLite implementation of Subscriptions.Jobs
"""

__all__ = []
__revision__ = "$Id: Jobs.py,v 1.5 2009/01/16 22:27:37 sfoulkes Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.WMBS.MySQL.Subscriptions.Jobs import Jobs as JobsMySQL

class Jobs(JobsMySQL):
    sql = JobsMySQL.sql
