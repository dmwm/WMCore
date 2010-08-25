#!/usr/bin/env python
"""
_JobsByState_

Monitoring DAO classes for Jobs in WMBS
"""
__all__ = []
__revision__ = "$Id: JobsBySubscription.py,v 1.1 2009/07/28 19:39:20 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Monitoring.JobsBySubscription import JobsBySubscription \
 as JobsBySubscriptionMySQL

class JobsBySubscription(JobsBySubscriptionMySQL):
    
    sql = JobsBySubscriptionMySQL.sql