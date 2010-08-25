#!/usr/bin/env python
"""
_SucceededJobs_

SQLite implementation of Subscriptions.SucceededJobs
"""

__all__ = []
__revision__ = "$Id: SucceededJobs.py,v 1.1 2010/06/01 21:14:18 riahi Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.SucceededJobs import SucceededJobs as SucceededJobsMySQL

class SucceededJobs(SucceededJobsMySQL):
    sql = LoadFromTaskMySQL.sql


