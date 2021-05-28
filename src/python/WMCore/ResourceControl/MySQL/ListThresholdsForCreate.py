#!/usr/bin/env python
"""
_ListThresholdsForCreate_

Query the database to determine how many jobs are pending so that we can
determine whether or not to task for more work.
"""

from future.utils import viewitems

from WMCore.Database.DBFormatter import DBFormatter


class ListThresholdsForCreate(DBFormatter):
    assignedSQL = """SELECT wmbs_location.site_name, wmbs_location.pending_slots,
                            wmbs_location.cms_name, wmbs_location.plugin,
                            COUNT(wmbs_job.id) AS total, wls.name AS state,
                            runjob.status AS job_status, wmbs_workflow.priority
                            FROM wmbs_job
                       INNER JOIN wmbs_job_state ON
                         wmbs_job.state = wmbs_job_state.id
                       INNER JOIN wmbs_location ON
                         wmbs_job.location = wmbs_location.id
                       INNER JOIN wmbs_location_state wls ON
                         wls.id = wmbs_location.state
                       INNER JOIN wmbs_jobgroup ON
                         wmbs_jobgroup.id = wmbs_job.jobgroup
                       INNER JOIN wmbs_subscription ON
                         wmbs_subscription.id = wmbs_jobgroup.subscription
                       INNER JOIN wmbs_workflow ON
                         wmbs_workflow.id = wmbs_subscription.workflow
                       LEFT OUTER JOIN (SELECT wmbs_id AS id, bl_status.name as status
                                        FROM bl_runjob
                                            INNER JOIN bl_status ON
                                                bl_status.id = bl_runjob.sched_status
                                        WHERE bl_runjob.status = '1') runjob
                       ON runjob.id = wmbs_job.id
                     WHERE wmbs_job_state.name != 'success' AND
                           wmbs_job_state.name != 'complete' AND
                           wmbs_job_state.name != 'exhausted' AND
                           wmbs_job_state.name != 'cleanout' AND
                           wmbs_job_state.name != 'killed'
                     GROUP BY wmbs_location.site_name,
                     wmbs_location.pending_slots, wmbs_location.cms_name,
                     wls.name, runjob.status, wmbs_location.plugin,
                     wmbs_workflow.priority"""

    unassignedSQL = """SELECT wl.site_name, wl.pending_slots,
                              wl.cms_name, wls.name AS state,
                              COUNT(unassigned_jobs.job) AS job_count, unassigned_jobs.priority
                              FROM wmbs_location wl
                         LEFT OUTER JOIN
                           (SELECT DISTINCT wmbs_job_assoc.job, wmbs_location_pnns.location,
                                            wmbs_workflow.priority
                              FROM wmbs_job_assoc
                              INNER JOIN wmbs_file_location wfl ON
                                wmbs_job_assoc.fileid = wfl.fileid
                              INNER JOIN wmbs_location_pnns ON
                                wmbs_location_pnns.pnn = wfl.pnn
                              INNER JOIN wmbs_job ON
                                wmbs_job_assoc.job = wmbs_job.id
                              INNER JOIN wmbs_job_state ON
                                wmbs_job.state = wmbs_job_state.id
                              INNER JOIN wmbs_jobgroup ON
                                wmbs_job.jobgroup = wmbs_jobgroup.id
                              INNER JOIN wmbs_subscription ON
                                wmbs_jobgroup.subscription = wmbs_subscription.id
                              INNER JOIN wmbs_workflow ON
                                wmbs_workflow.id = wmbs_subscription.workflow
                              LEFT OUTER JOIN wmbs_subscription_validation wsv
                                ON wsv.location_id = wmbs_location_pnns.location
                                AND wsv.subscription_id = wmbs_jobgroup.subscription
                            WHERE wmbs_job.location IS NULL AND
                                  wmbs_job_state.name != 'killed' AND
                                  wmbs_job_state.name != 'cleanout' AND
                                  (wsv.valid = 1 OR
                                   (wsv.valid IS NULL AND NOT EXISTS
                                    (SELECT wsv2.valid FROM wmbs_subscription_validation wsv2
                                     WHERE wsv2.subscription_id = wmbs_jobgroup.subscription
                                     AND wsv2.valid = 1)))) unassigned_jobs ON
                            wl.id = unassigned_jobs.location
                         INNER JOIN wmbs_location_state wls ON
                            wls.id = wl.state
                            GROUP BY wl.site_name, wl.pending_slots,
                            wl.cms_name, wls.name, unassigned_jobs.priority"""

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
            siteName = result["site_name"]

            if not siteName in results:
                results[siteName] = {"cms_name": result["cms_name"],
                                     "state": result["state"],
                                     "pending_jobs": {},
                                     "total_slots": result["pending_slots"]}

            countJobs = True
            if result['job_status']:
                module = __import__("WMCore.BossAir.Plugins.%s" % result['plugin'],
                                    globals(), locals(), [result['plugin']])
                plugIn = getattr(module, result['plugin'])
                status = plugIn.stateMap().get(result['job_status'])
                if status == 'Running':
                    countJobs = False

            priority = result["priority"]

            if priority not in results[siteName]["pending_jobs"]:
                results[siteName]["pending_jobs"][priority] = 0

            if countJobs:
                results[siteName]["pending_jobs"][priority] += result["total"]

            results[result["site_name"]]["total_slots"] = result["pending_slots"]

        # Sum up all the jobs currently unassigned
        for result in unassignedResults:
            siteName = result['site_name']
            if siteName not in results:
                results[siteName] = {"total_slots": result["pending_slots"],
                                     "pending_jobs": {},
                                     "cms_name": result["cms_name"],
                                     "state": result["state"]}
            priority = result["priority"] or 0

            if priority not in results[siteName]['pending_jobs']:
                results[siteName]['pending_jobs'][priority] = 0
            results[siteName]['pending_jobs'][priority] += result['job_count']

        return results

    def formatTable(self, formattedResults):
        """
        _formatTable_
        """
        results = []
        for k, v in viewitems(formattedResults):
            item = {}
            item['site'] = k
            item.update(v)
            results.append(item)
        return results

    def execute(self, conn=None, transaction=False, tableFormat=False):
        assignedResults = self.dbi.processData(self.assignedSQL, conn=conn,
                                               transaction=transaction)
        unassignedResults = self.dbi.processData(self.unassignedSQL, conn=conn,
                                                 transaction=transaction)
        results = self.format(assignedResults, unassignedResults)
        if tableFormat:
            return self.formatTable(results)
        else:
            return results
