#!/usr/bin/env python
"""
_JobsByState_

Monitoring DAO classes for Jobs in WMBS
"""
__all__ = []



from WMCore.WMBS.MySQL.Monitoring.DefaultFormatter import DefaultFormatter

class JobCountByState(DefaultFormatter):
    sql = """SELECT count(wmbs_job.state) AS job_count, wmbs_job_state.name AS job_state 
             FROM wmbs_job
             INNER JOIN wmbs_job_state ON wmbs_job.state=wmbs_job_state.id 
             GROUP BY wmbs_job_state.name"""
