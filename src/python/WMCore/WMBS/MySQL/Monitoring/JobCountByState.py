#!/usr/bin/env python
"""
_JobsByState_

Monitoring DAO classes for Jobs in WMBS
"""
__all__ = []
__revision__ = "$Id: JobCountByState.py,v 1.2 2009/07/30 16:32:42 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Monitoring.DefaultFormatter import DefaultFormatter

class JobCountByState(DefaultFormatter):
    sql = """SELECT count(wmbs_job.state) AS job_count, wmbs_job_state.name AS job_state 
             FROM wmbs_job
             INNER JOIN wmbs_job_state ON wmbs_job.state=wmbs_job_state.id 
             GROUP BY wmbs_job_state.name"""
