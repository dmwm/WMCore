#!/usr/bin/env python
"""
_GetJobGroups_

MySQL implementation of Subscription.GetJobGroups
"""

__all__ = []
__revision__ = "$Id: GetJobGroups.py,v 1.2 2009/07/15 21:54:20 meloam Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

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
                                FROM wmbs_job job,
                                     wmbs_job_state jobstate
                                WHERE   job.state = jobstate.id
                                    AND job.jobgroup = jobgroup.id
                                    AND jobstate.name = "New")
                                <> (SELECT COUNT(*)
                                            FROM wmbs_job job2
                                            WHERE job2.jobgroup=jobgroup.id)
        """                                                 
        allJobs = self.formatFlat(self.dbi.processData(newstep, 
                           {"subscription": subscription},
                           conn = conn, transaction = transaction))
                                  
                                       
                                     
                          
        return allJobs
    def formatFlat(self, result):
        """
        Some standard formatting, put all records into a list
        """
        out = []
        for r in result:
            for i in r.fetchall():
                for j in i:
                    out.append(j)    
            r.close()
        return out
                
  