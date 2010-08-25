#!/usr/bin/env python
"""
_GetJobGroups_

MySQL implementation of Subscription.GetJobGroups
"""

__all__ = []
__revision__ = "$Id: GetJobGroups.py,v 1.4 2009/08/03 19:46:37 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WMBS.JobGroup import JobGroup
class GetJobGroups(DBFormatter):
    
    def execute(self, subscription = None, conn = None, transaction = False):
        # my logic
        # 1) foreach jobgroup within the subscription
        #  2) do a subquery to find the number of jobs that are acquired/failed
        #  3) check to see if this number of jobs is less than the total number                           
        newstep = """SELECT wmbs_jobgroup.id  FROM wmbs_jobgroup
                       INNER JOIN
                         (SELECT wmbs_job.jobgroup AS jobgroup, COUNT(*) AS new_jobs FROM wmbs_job
                          WHERE wmbs_job.state = (SELECT id FROM wmbs_job_state WHERE name = 'new')
                          GROUP BY wmbs_job.jobgroup) new_count ON
                          wmbs_jobgroup.id = new_count.jobgroup
                        WHERE wmbs_jobgroup.subscription = :subscription
                        AND new_count.new_jobs != 0
        """                                                 
        allJobs = self.formatFlat(self.dbi.processData(newstep, 
                           {"subscription": subscription},
                           conn = conn, transaction = transaction))

        return allJobs
                                  
                                       
                                     
                          
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

  
