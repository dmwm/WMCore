#!/usr/bin/env python
"""
_JobsByState_

Monitoring DAO classes for Jobs in WMBS
"""
__all__ = []
__revision__ = "$Id: JobsByState.py,v 1.2 2009/07/30 16:32:42 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Monitoring.DefaultFormatter import DefaultFormatter

# Job Related Query
class JobsByState(DefaultFormatter):
    
    #TO check what else is needed for return item
    sql = """SELECT wmbs_job.id AS jobID, wmbs_job.name AS job_name, wmbs_job_state.name AS job_state 
             FROM wmbs_job 
             INNER JOIN wmbs_job_state ON wmbs_job.state=wmbs_job_state.id ORDER BY state"""
