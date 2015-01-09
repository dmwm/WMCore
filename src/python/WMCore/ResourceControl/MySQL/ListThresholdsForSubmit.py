#!/usr/bin/env python
"""
_ListThresholdsForSubmit_

Query WMBS and ResourceControl to determine how many jobs are still running so
that we can schedule jobs that have just been created.
"""

from WMCore.Database.DBFormatter import DBFormatter

class ListThresholdsForSubmit(DBFormatter):
    sql = """SELECT wmbs_location.site_name AS site_name,
                    wmbs_location.pending_slots,
                    wmbs_location.running_slots,
                    wmbs_location.cms_name AS cms_name,
                    rc_threshold.max_slots,
                    rc_threshold.pending_slots AS task_pending_slots,
                    wmbs_sub_types.name AS task_type,
                    job_count.job_status,
                    job_count.jobs,
                    wmbs_sub_types.priority,
                    wmbs_location_state.name AS state,
                    wmbs_location.plugin AS plugin
                    FROM wmbs_location
               INNER JOIN rc_threshold ON
                 wmbs_location.id = rc_threshold.site_id
               INNER JOIN wmbs_sub_types ON
                 rc_threshold.sub_type_id = wmbs_sub_types.id
               INNER JOIN wmbs_location_state ON
                wmbs_location_state.id = wmbs_location.state
               LEFT OUTER JOIN
                 (SELECT wmbs_job.location AS location,
                         wmbs_subscription.subtype AS subtype,
                         COUNT(wmbs_job.id) as jobs,
                         bl_status.name as job_status
                         FROM wmbs_job
                    INNER JOIN wmbs_jobgroup ON
                      wmbs_job.jobgroup = wmbs_jobgroup.id
                    INNER JOIN wmbs_subscription ON
                      wmbs_jobgroup.subscription = wmbs_subscription.id
                    INNER JOIN wmbs_job_state ON
                      wmbs_job.state = wmbs_job_state.id
                    INNER JOIN bl_runjob ON
                      bl_runjob.wmbs_id = wmbs_job.id
                    INNER JOIN bl_status ON
                      bl_status.id = bl_runjob.sched_status
                  WHERE wmbs_job_state.name = 'executing' AND
                        bl_runjob.status = '1'
                  GROUP BY wmbs_job.location, wmbs_subscription.subtype,
                           bl_status.name) job_count ON
                  wmbs_location.id = job_count.location AND
                  wmbs_sub_types.id = job_count.subtype
               ORDER BY wmbs_sub_types.priority DESC"""
    seSql = """SELECT wl.site_name AS site_name,
                      wls.se_name AS pnn
               FROM wmbs_location wl
                      INNER JOIN wmbs_location_senames wls ON
                          wls.location = wl.id
            """


    def format(self, results, storageElements):
        """
        _format_

        Add up totals from various rows and combine that together into a single
        data structure.
        """
        results = DBFormatter.formatDict(self, results)
        storageElements = DBFormatter.formatDict(self, storageElements)

        mappedPNNs = {}
        for pnn in storageElements:
            if pnn['site_name'] not in mappedPNNs:
                mappedPNNs[pnn['site_name']] = []
            mappedPNNs[pnn['site_name']].append(pnn['pnn'])

        formattedResults = {}
        for result in results:
            siteName = result['site_name']
            taskType = result['task_type']

            task_pending_jobs = 0
            task_running_jobs = 0
            if result['job_status']:
                module = __import__("WMCore.BossAir.Plugins.%s" % result['plugin'],
                                globals(), locals(), [result['plugin']])
                plugIn = getattr(module, result['plugin'])
                status = plugIn.stateMap().get(result['job_status'])
                if status == 'Pending':
                    task_pending_jobs  += result['jobs']
                elif status == 'Running':
                    task_running_jobs += result['jobs']

            if siteName not in formattedResults:
                siteInfo = {}
                formattedResults[siteName] = siteInfo
                siteInfo['cms_name']             = result['cms_name']
                siteInfo['pnns']                 = mappedPNNs.get(siteName, [])
                siteInfo['state']                = result['state']
                siteInfo['total_pending_slots']  = result['pending_slots']
                siteInfo['total_running_slots']  = result['running_slots']
                siteInfo['total_pending_jobs']   = task_pending_jobs
                siteInfo['total_running_jobs']   = task_running_jobs
                siteInfo['thresholds']           = []
            else:
                formattedResults[siteName]['total_pending_jobs'] += task_pending_jobs
                formattedResults[siteName]['total_running_jobs'] += task_running_jobs

            for threshold in formattedResults[siteName]['thresholds']:
                if threshold['task_type'] == taskType:
                    threshold['task_running_jobs'] += task_running_jobs
                    threshold['task_pending_jobs'] += task_pending_jobs
                    break
            else:
                threshold = {}
                threshold['task_type']               = taskType
                threshold['max_slots']               = result['max_slots']
                threshold['pending_slots']           = result['task_pending_slots']
                threshold['priority']                = result['priority']
                threshold['task_running_jobs']       = task_running_jobs
                threshold['task_pending_jobs']       = task_pending_jobs
                formattedResults[siteName]['thresholds'].append(threshold)


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
        pnns    = self.dbi.processData(self.seSql, conn = conn, transaction = transaction)
        results = self.format(results, pnns)

        if tableFormat:
            return self.formatTable(results)
        else:
            return results
