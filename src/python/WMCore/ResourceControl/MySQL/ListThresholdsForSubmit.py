#!/usr/bin/env python
"""
_ListThresholdsForSubmit_

Query WMBS and ResourceControl to determine how many jobs are still running so
that we can schedule jobs that have just been created.
"""




from WMCore.Database.DBFormatter import DBFormatter

class ListThresholdsForSubmit(DBFormatter):
    sql = """SELECT wmbs_location.site_name AS site_name,
                    wmbs_location.se_name AS se_name,
                    wmbs_location.job_slots,
                    rc_threshold.max_slots,
                    wmbs_sub_types.name AS task_type,
                    job_count.total AS task_running_jobs
                    FROM wmbs_location
               INNER JOIN rc_threshold ON
                 wmbs_location.id = rc_threshold.site_id
               INNER JOIN wmbs_sub_types ON
                 rc_threshold.sub_type_id = wmbs_sub_types.id
               LEFT OUTER JOIN
                 (SELECT wmbs_job.location AS location, wmbs_subscription.subtype AS subtype,
                         COUNT(wmbs_job.id) AS total FROM wmbs_job
                    INNER JOIN wmbs_jobgroup ON
                      wmbs_job.jobgroup = wmbs_jobgroup.id
                    INNER JOIN wmbs_subscription ON
                      wmbs_jobgroup.subscription = wmbs_subscription.id
                    INNER JOIN wmbs_job_state ON
                      wmbs_job.state = wmbs_job_state.id
                  WHERE wmbs_job_state.name = 'executing'
                  GROUP BY wmbs_job.location, wmbs_subscription.subtype) job_count ON
                  wmbs_location.id = job_count.location AND
                  wmbs_sub_types.id = job_count.subtype"""

    def format(self, results):
        """
        _format_

        Add up totals from various rows and combine that together into a single
        data structure.
        """
        results = DBFormatter.formatDict(self, results)

        formattedResults = {}
        totalRunning = {}
        for result in results:
            if not formattedResults.has_key(result["site_name"]):
                formattedResults[result["site_name"]] = {}
                totalRunning[result["site_name"]] = 0
            if not formattedResults[result["site_name"]].has_key(result["task_type"]):
                formattedResults[result["site_name"]][result["task_type"]] = {}

            if result["task_running_jobs"] == None:
                result["task_running_jobs"] = 0
                
            totalRunning[result["site_name"]] += result["task_running_jobs"]
            formattedResult = formattedResults[result["site_name"]][result["task_type"]]
            formattedResult["total_slots"] = result["job_slots"]
            formattedResult["task_running_jobs"] = result["task_running_jobs"]
            formattedResult["max_slots"] = result["max_slots"]
            formattedResult["se_name"]   = result["se_name"]

        for siteName in totalRunning.keys():
            for taskType in formattedResults[siteName].keys():
                formattedResults[siteName][taskType]["total_running_jobs"] = totalRunning[siteName]

        return formattedResults
    
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
            item['data'] = []
            for ck, cv in v.items():
                childItem = {}
                childItem['type'] = ck
                childItem.update(cv)
                item['data'].append(childItem)
            results.append(item)
        return {'results': results}
    
    def execute(self, conn = None, transaction = False, tableFormat = False):
        results = self.dbi.processData(self.sql, conn = conn, transaction = transaction)
        results = self.format(results)

        if tableFormat:
            return self.formatTable(results)
        else:
            return results
