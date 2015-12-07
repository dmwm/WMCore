#!/usr/bin/env python
"""
_ThresholdBySite_

Query WMBS, ResourceControl and BossAir to give information about the thresholds
and the number of running jobs.
"""

from WMCore.Database.DBFormatter import DBFormatter

class ThresholdBySite(DBFormatter):
    sql = """SELECT wmbs_location.site_name AS site_name,
                    wmbs_location.pending_slots,
                    wmbs_location.running_slots,
                    rc_threshold.max_slots,
                    rc_threshold.pending_slots AS task_pending_slots,
                    wmbs_sub_types.name AS task_type,
                    job_count.running_jobs AS task_running_jobs,
                    job_count.pending_jobs AS task_pending_jobs
                    FROM wmbs_location
               INNER JOIN rc_threshold ON
                 wmbs_location.id = rc_threshold.site_id
               INNER JOIN wmbs_sub_types ON
                 rc_threshold.sub_type_id = wmbs_sub_types.id
               LEFT OUTER JOIN
                 (SELECT wmbs_job.location AS location, wmbs_subscription.subtype AS subtype,
                         SUM(CASE WHEN bl_runjob.sched_status =
                                 (SELECT id FROM bl_status where name = 'Running')
                             THEN 1 ELSE 0 END) AS running_jobs,
                         SUM(CASE WHEN bl_runjob.sched_status =
                             (SELECT id FROM bl_status where name = 'Idle')
                             THEN 1 ELSE 0 END) AS pending_jobs
                         FROM wmbs_job
                    INNER JOIN wmbs_jobgroup ON
                      wmbs_job.jobgroup = wmbs_jobgroup.id
                    INNER JOIN wmbs_subscription ON
                      wmbs_jobgroup.subscription = wmbs_subscription.id
                    INNER JOIN wmbs_job_state ON
                      wmbs_job.state = wmbs_job_state.id
                    INNER JOIN bl_runjob ON
                      bl_runjob.wmbs_id = wmbs_job.id
                  WHERE wmbs_job_state.name = 'executing' AND
                        bl_runjob.status = '1'
                  GROUP BY wmbs_job.location, wmbs_subscription.subtype) job_count ON
                  wmbs_location.id = job_count.location AND
                  wmbs_sub_types.id = job_count.subtype
              WHERE wmbs_location.site_name = :site"""

    def execute(self, site, conn = None, transaction = False, tableFormat = False):
        results = self.dbi.processData(self.sql, {'site': site},
                                       conn = conn, transaction = transaction)
        return self.formatDict(results)
