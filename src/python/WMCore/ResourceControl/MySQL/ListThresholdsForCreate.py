#!/usr/bin/env python
"""
_ListThresholdsForCreate_

Query the database to determine how many jobs are running so that we can
determine whether or not to task for more work.  
"""

__revision__ = "$Id: ListThresholdsForCreate.py,v 1.2 2010/02/11 21:53:50 sfoulkes Exp $"
__version__  = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class ListThresholdsForCreate(DBFormatter):
    assignedSQL = """SELECT wmbs_location.site_name, wmbs_location.job_slots,
                            COUNT(wmbs_job.id) AS total FROM wmbs_job
                       INNER JOIN wmbs_job_state ON
                         wmbs_job.state = wmbs_job_state.id
                       INNER JOIN wmbs_location ON
                         wmbs_job.location = wmbs_location.id
                     WHERE wmbs_job_state.name != 'success' AND
                           wmbs_job_state.name != 'complete' AND
                           wmbs_job_state.name != 'exhausted' AND
                           wmbs_job_state.name != 'cleanout'
                     GROUP BY wmbs_location.site_name, wmbs_location.job_slots"""
    
    unassignedSQL = """SELECT wmbs_location.site_name, wmbs_location.job_slots,
                              job_count.total AS total FROM wmbs_location
                         LEFT OUTER JOIN
                           (SELECT wmbs_subscription_location.location, COUNT(wmbs_job.id) AS total
                                   FROM wmbs_job
                              INNER JOIN wmbs_jobgroup ON
                                wmbs_job.jobgroup = wmbs_jobgroup.id
                              INNER JOIN wmbs_subscription_location ON
                                wmbs_jobgroup.subscription = wmbs_subscription_location.subscription
                            WHERE wmbs_job.location IS NULL    
                            GROUP BY wmbs_subscription_location.location) job_count ON
                            wmbs_location.id = job_count.location"""
    
    def format(self, assignedResults, unassignedResults):
        """
        _format_

        Combine together the total we received from the assigned and unassigned
        queries into a single datastructure.
        """
        assignedResults = DBFormatter.formatDict(self, assignedResults)
        unassignedResults = DBFormatter.formatDict(self, unassignedResults)

        results = {}
        for result in assignedResults:
            if result["total"] == None:
                result["total"] = 0
                
            if not results.has_key(result["site_name"]):
                results[result["site_name"]] = {"total_slots": 0, "running_jobs": 0}

            results[result["site_name"]]["running_jobs"] += result["total"]
            results[result["site_name"]]["total_slots"] = result["job_slots"]

        for result in unassignedResults:
            if result["total"] == None:
                result["total"] = 0
                
            if not results.has_key(result["site_name"]):
                results[result["site_name"]] = {"total_slots": 0, "running_jobs": 0}

            results[result["site_name"]]["running_jobs"] += result["total"]
            results[result["site_name"]]["total_slots"] = result["job_slots"]

        return results
    
    def execute(self, conn = None, transaction = False):
        assignedResults = self.dbi.processData(self.assignedSQL, conn = conn,
                                               transaction = transaction)
        unassignedResults = self.dbi.processData(self.unassignedSQL, conn = conn,
                                                 transaction = transaction)        
        return self.format(assignedResults, unassignedResults)
