#!/usr/bin/env python
"""
_Jobs_

Oracle implementation of Subscriptions.Jobs

Return a list of all jobs that exist for a subscription.
"""

__all__ = []
__revision__ = "$Id: Jobs.py,v 1.4 2009/01/12 19:26:05 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMBS.MySQL.Subscriptions.Jobs import Jobs as JobsMySQL

class Jobs(JobsMySQL):
    sql = JobsMySQL.sql
