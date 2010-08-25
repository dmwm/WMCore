#!/usr/bin/env python
"""
_JobCountBySubscription_

Monitoring DAO classes for Jobs in WMBS
"""
__all__ = []
__revision__ = "$Id: JobCountBySubscriptionAndRun.py,v 1.1 2009/07/28 19:39:20 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Monitoring.JobCountBySubscriptionAndRun \
  import JobCountBySubscriptionAndRun as JobCountBySubscriptionAndRunMySQL

class JobCountBySubscriptionAndRun(JobCountBySubscriptionAndRunMySQL):
    """
    _JobCountBySubscription_
    
    return the number of jobs grouped by their status and run for given a subscription (fileset, workflow pair)  
    """
    #Oracle keyword replacement - not very good way.
    sql = JobCountBySubscriptionAndRunMySQL.sql.replace('FILE', 'FILEID')