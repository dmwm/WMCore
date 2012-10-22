#!/usr/bin/env python
"""
_JobStatusForMonitoring_

Oracle implementation for loading a job by scheduler status
"""

from WMCore.BossAir.MySQL.JobStatusForMonitoring \
     import JobStatusForMonitoring as MySQLJobStatusForMonitoring

class JobStatusForMonitoring(MySQLJobStatusForMonitoring):
    sql = """SELECT wwf.name as workflow, count(rj.wmbs_id) AS num_jobs,
                    st.name AS status, wl.plugin AS plugin, wu.cert_dn AS owner
               FROM bl_runjob rj
               INNER JOIN wmbs_users wu ON wu.id = rj.user_id
               INNER JOIN bl_status st ON rj.sched_status = st.id
               INNER JOIN wmbs_job wj ON wj.id = rj.wmbs_id
               INNER JOIN wmbs_jobgroup wjg ON wjg.id = wj.jobgroup
               INNER JOIN wmbs_subscription ws ON ws.id = wjg.subscription
               INNER JOIN wmbs_workflow wwf ON wwf.id = ws.workflow
               LEFT OUTER JOIN wmbs_location wl ON wl.id = wj.location
               WHERE rj.status = :complete
               GROUP BY wwf.name, wl.plugin, st.name, wu.cert_dn
    """
