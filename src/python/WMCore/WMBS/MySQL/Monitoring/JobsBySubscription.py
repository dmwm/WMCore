#!/usr/bin/env python
"""
_JobsByState_

Monitoring DAO classes for Jobs in WMBS
"""
__all__ = []
__revision__ = "$Id: JobsBySubscription.py,v 1.2 2009/07/30 16:32:42 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Monitoring.DefaultFormatter import DefaultFormatter

class JobsBySubscription(DefaultFormatter):
    
    sql = """SELECT wmbs_job.id AS job_id, wjs.name as job_state, wmbs_job.state_time AS state_time  
              FROM wmbs_job
               INNER JOIN wmbs_jobgroup wjg ON wmbs_job.jobgroup = wjg.id 
               INNER JOIN wmbs_subscription ws ON wjg.subscription = ws.id 
               INNER JOIN wmbs_fileset wfs ON ws.fileset = wfs.id
               INNER JOIN wmbs_workflow ww ON ws.workflow = ww.id
               INNER JOIN wmbs_job_state wjs ON wmbs_job.state = wjs.id 
              WHERE wfs.name = :fileset_name AND ww.name = :workflow_name AND wmbs_job.state_time > :state_time
              ORDER by job_id"""
              
    def execute(self, fileset_name, workflow_name, state_time, conn = None, transaction = False):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        """        
        state_time = int(state_time)
        bindVars = {"fileset_name": fileset_name, 'workflow_name': workflow_name,"state_time": state_time}
        
        result = self.dbi.processData(self.sql, bindVars, conn = conn,
                                      transaction = transaction)
        
        return self.formatDict(result)
