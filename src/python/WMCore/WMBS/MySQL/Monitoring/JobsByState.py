#!/usr/bin/env python
"""
_JobsByState_

Monitoring DAO classes for Jobs in WMBS
"""
__all__ = []



from WMCore.WMBS.MySQL.Monitoring.DefaultFormatter import DefaultFormatter

# Job Related Query
class JobsByState(DefaultFormatter):

    #TO check what else is needed for return item
    sql = """SELECT wmbs_job.id AS jobID, wmbs_job.name AS job_name, wmbs_job_state.name AS job_state
             FROM wmbs_job
             INNER JOIN wmbs_job_state ON wmbs_job.state=wmbs_job_state.id ORDER BY state"""
