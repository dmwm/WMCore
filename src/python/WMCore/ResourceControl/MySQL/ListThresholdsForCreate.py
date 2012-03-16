#!/usr/bin/env python
"""
_ListThresholdsForCreate_

Query the database to determine how many jobs are running so that we can
determine whether or not to task for more work.  
"""

import copy

from WMCore.Database.DBFormatter import DBFormatter

class ListThresholdsForCreate(DBFormatter):
    assignedSQL = """SELECT wmbs_location.site_name, wmbs_location.job_slots, wmbs_location.cms_name,
                            COUNT(wmbs_job.id) AS total, wmbs_location.drain FROM wmbs_job
                       INNER JOIN wmbs_job_state ON
                         wmbs_job.state = wmbs_job_state.id
                       INNER JOIN wmbs_location ON
                         wmbs_job.location = wmbs_location.id
                     WHERE wmbs_job_state.name != 'success' AND
                           wmbs_job_state.name != 'complete' AND
                           wmbs_job_state.name != 'exhausted' AND
                           wmbs_job_state.name != 'cleanout' AND
                           wmbs_job_state.name != 'killed'
                     GROUP BY wmbs_location.site_name, wmbs_location.job_slots"""
    
    unassignedSQL = """SELECT wmbs_location.site_name, wmbs_location.job_slots,
                              wmbs_location.cms_name, wmbs_location.drain,
                              COUNT(unassigned_jobs.job) AS job_count FROM wmbs_location
                         LEFT OUTER JOIN
                           (SELECT DISTINCT wmbs_job_assoc.job, wmbs_file_location.location
                              FROM wmbs_job_assoc
                              INNER JOIN wmbs_file_location ON
                                wmbs_job_assoc.fileid = wmbs_file_location.fileid
                              INNER JOIN wmbs_job ON
                                wmbs_job_assoc.job = wmbs_job.id
                              INNER JOIN wmbs_job_state ON
                                wmbs_job.state = wmbs_job_state.id
                              INNER JOIN wmbs_jobgroup ON
                                wmbs_job.jobgroup = wmbs_jobgroup.id
                              LEFT OUTER JOIN wmbs_subscription_validation wsv
                                ON wsv.location_id = wmbs_file_location.location
                                AND wsv.subscription_id = wmbs_jobgroup.subscription
                            WHERE wmbs_job.location IS NULL AND
                                  wmbs_job_state.name != 'killed' AND
                                  wmbs_job_state.name != 'cleanout' AND
                                  (wsv.valid = 1 OR
                                   (wsv.valid IS NULL AND NOT EXISTS
                                    (SELECT wsv2.valid FROM wmbs_subscription_validation wsv2
                                     WHERE wsv2.subscription_id = wmbs_jobgroup.subscription
                                     AND wsv2.valid = 1)))) unassigned_jobs ON
                            wmbs_location.id = unassigned_jobs.location
                            GROUP BY wmbs_location.site_name"""
    
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
                if result['drain'] == 'T':
                    drainValue = True
                else:
                    drainValue = False
                results[result["site_name"]] = {"total_slots": 0, "running_jobs": 0,
                                                "cms_name": result["cms_name"],
                                                "drain" : drainValue}

            results[result["site_name"]]["running_jobs"] += result["total"]
            results[result["site_name"]]["total_slots"] = result["job_slots"]

        # Sum up all the jobs currently unassigned
        for result in unassignedResults:
            siteName = result['site_name']
            if not results.has_key(siteName):
                if result['drain'] == 'T':
                    drainValue = True
                else:
                    drainValue = False
                results[siteName] = {"total_slots": result["job_slots"],
                                     "running_jobs": 0,
                                     "cms_name": result["cms_name"],
                                     "drain" : drainValue}
            results[siteName]['running_jobs'] += result['job_count']

        return results
    
    def formatTable(self, formattedResults):
        """
        _formatTable_
        """
        results = []
        for k, v in formattedResults.items():
            item = {}
            item['site'] = k
            item.update(v)
            results.append(item)
        return results
    
    def execute(self, conn = None, transaction = False, tableFormat = False):
        assignedResults = self.dbi.processData(self.assignedSQL, conn = conn,
                                               transaction = transaction)
        unassignedResults = self.dbi.processData(self.unassignedSQL, conn = conn,
                                                 transaction = transaction)
        results = self.format(assignedResults, unassignedResults)
        if tableFormat:
            return self.formatTable(results)
        else:
            return results
