#!/usr/bin/env python
"""
_JobsByState_

Monitoring DAO classes for Jobs in WMBS
"""
__all__ = []
__revision__ = "$Id: JobCountByState.py,v 1.1 2009/07/28 19:38:30 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class JobCountByState(DBFormatter):
    sql = """SELECT count(wmbs_job.state) AS count, wmbs_job_state.name AS name 
             FROM wmbs_job
             INNER JOIN wmbs_job_state ON wmbs_job.state=wmbs_job_state.id 
             GROUP BY wmbs_job_state.name"""
