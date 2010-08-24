#!/usr/bin/env python
"""
_ListThresholdsForCreate_

Query the database to determine how many jobs are running so that we can
determine whether or not to task for more work.  
"""




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
                              COUNT(job_location.location) AS total FROM wmbs_location
                         LEFT OUTER JOIN
                           (SELECT wmbs_job_assoc.job, wmbs_file_location.location AS location
                                   FROM wmbs_job_assoc
                              INNER JOIN wmbs_file_location ON
                                wmbs_job_assoc.file = wmbs_file_location.file
                              INNER JOIN wmbs_job ON
                                wmbs_job_assoc.job = wmbs_job.id
                            WHERE wmbs_job.location IS NULL) job_location ON
                            wmbs_location.id = job_location.location
                       GROUP BY wmbs_location.site_name, wmbs_location.job_slots"""
    
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
    
    def formatTable(self, formattedResults):
        """
        _format_

        Combine together the total we received from the assigned and unassigned
        queries into a single datastructure.
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
