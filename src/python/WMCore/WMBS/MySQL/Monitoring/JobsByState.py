#!/usr/bin/env python
"""
_JobsByState_

Monitoring DAO classes for Jobs in WMBS
"""
__all__ = []
__revision__ = "$Id: JobsByState.py,v 1.1 2009/07/28 19:38:30 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

## T0 specific Run related Query
#class RunsByState(DBFormatter):
#    
#    #TO: check what else is needed for return item
#    sql = """SELECT run_id, run.start_time, run_status.name FROM run  
#             INNER JOIN run_status  
#              ON run.run_status = run_status.id 
#             GROUP BY run_status.status"""


# Job Related Query
class JobsByState(DBFormatter):
    
    #TO check what else is needed for return item
    sql = """SELECT wmbs_job.id, wmbs_job.name AS name, wmbs_job_state.name AS state 
             FROM wmbs_job 
             INNER JOIN wmbs_job_state ON wmbs_job.state=wmbs_job_state.id ORDER BY state"""
