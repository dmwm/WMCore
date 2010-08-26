#!/usr/bin/env python
"""
_JobsByState_

Monitoring DAO classes for Jobs in WMBS
"""
__all__ = []
__revision__ = "$Id: JobCountByState.py,v 1.1 2009/07/28 19:39:20 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Monitoring.JobCountByState import JobCountByState \
 as JobCountByStateMySQL

class JobCountByState(JobCountByStateMySQL):
    sql = JobCountByStateMySQL.sql