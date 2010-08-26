#!/usr/bin/env python
"""
_SucceededJobs_

SQLite implementation of Subscriptions.SucceededJobs
"""

__all__ = []
__revision__ = "$Id: SucceededJobs.py,v 1.2 2010/06/28 19:04:09 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Subscriptions.SucceededJobs import SucceededJobs as SucceededJobsMySQL

class SucceededJobs(SucceededJobsMySQL):
    sql = SucceededJobsMySQL.sql


