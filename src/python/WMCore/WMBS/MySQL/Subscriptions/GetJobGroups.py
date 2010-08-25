#!/usr/bin/env python
"""
_GetJobGroups_

MySQL implementation of Subscription.GetJobGroups
"""

__all__ = []
__revision__ = "$Id: GetJobGroups.py,v 1.3 2009/07/17 20:21:35 meloam Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WMBS.JobGroup import JobGroup
class GetJobGroups(DBFormatter):
    
    def execute(self, subscription = None, conn = None, transaction = False):
        # my logic
        # 1) foreach jobgroup within the subscription
        #  2) do a subquery to find the number of jobs that are acquired/failed
        #  3) check to see if this number of jobs is less than the total number                           
        newstep = """SELECT jobgroup.id
                        FROM  wmbs_jobgroup jobgroup
                        WHERE jobgroup.subscription = :subscription
                            AND (SELECT COUNT(*) 
                                FROM wmbs_job job
                                WHERE   job.state = (select id from wmbs_job_state where name = "new")
                                    AND job.jobgroup = jobgroup.id)
                                <> 0
        """                                                 
        allJobs = self.formatFlat(self.dbi.processData(newstep, 
                           {"subscription": subscription},
                           conn = conn, transaction = transaction))
                                  
                                       
                                     
                          
        return allJobs

    def formatFlat(self, result):
        """
        Some standard formatting, put all records into a list.
        """
        out = []
        for r in result:
            for i in r.fetchall():
                for j in i:
                    newJobGroup = JobGroup(id = j)
                    newJobGroup.loadData()
                    out.append(newJobGroup)    
            r.close()
        return out

  