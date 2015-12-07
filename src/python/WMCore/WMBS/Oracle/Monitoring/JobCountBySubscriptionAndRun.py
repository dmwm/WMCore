#!/usr/bin/env python
"""
_JobCountBySubscription_

Monitoring DAO classes for Jobs in WMBS
"""
__all__ = []



from WMCore.WMBS.MySQL.Monitoring.JobCountBySubscriptionAndRun \
  import JobCountBySubscriptionAndRun as JobCountBySubscriptionAndRunMySQL

class JobCountBySubscriptionAndRun(JobCountBySubscriptionAndRunMySQL):
    """
    _JobCountBySubscription_

    return the number of jobs grouped by their status and run for given a subscription (fileset, workflow pair)
    """
