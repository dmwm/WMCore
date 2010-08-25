#!/usr/bin/env python
"""
_CompleteInput_

Oracle implementation of Jobs.Complete
"""

__revision__ = "$Id: CompleteInput.py,v 1.1 2009/10/13 20:52:41 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.CompleteInput import CompleteInput as MySQLCompleteInput

class CompleteInput(MySQLCompleteInput):
    """
    _CompleteInput_

    """
    acquiredDelete = """DELETE FROM wmbs_sub_files_acquired
                          WHERE subscription =
                            (SELECT wmbs_subscription.id FROM wmbs_subscription
                               INNER JOIN wmbs_jobgroup ON
                                 wmbs_subscription.id = wmbs_jobgroup.subscription
                               INNER JOIN wmbs_job ON
                                 wmbs_jobgroup.id = wmbs_job.jobgroup
                             WHERE wmbs_job.id = :jobid) AND fileid IN
                               (SELECT fileid FROM
                                  (SELECT wmbs_job_assoc.fileid AS fileid, wmbs_jobgroup.subscription AS subscription,
                                          COUNT(*) AS total_jobs, SUM(wmbs_job.outcome) AS successful_jobs
                                          FROM wmbs_job_assoc
                                     INNER JOIN
                                       (SELECT fileid FROM wmbs_job_assoc WHERE job = :jobid) job_input ON
                                       wmbs_job_assoc.fileid = job_input.fileid
                                     INNER JOIN wmbs_job ON
                                       wmbs_job_assoc.job = wmbs_job.id
                                     INNER JOIN wmbs_jobgroup ON
                                       wmbs_job.jobgroup = wmbs_jobgroup.id
                                   WHERE wmbs_jobgroup.subscription =
                                     (SELECT subscription FROM wmbs_jobgroup
                                        INNER JOIN wmbs_job ON
                                          wmbs_jobgroup.id = wmbs_job.jobgroup
                                      WHERE wmbs_job.id = :jobid)
                                   GROUP BY wmbs_job_assoc.fileid, wmbs_jobgroup.subscription)
                                WHERE total_jobs = successful_jobs)"""

    failedDelete = """DELETE FROM wmbs_sub_files_failed
                          WHERE subscription =
                            (SELECT wmbs_subscription.id FROM wmbs_subscription
                               INNER JOIN wmbs_jobgroup ON
                                 wmbs_subscription.id = wmbs_jobgroup.subscription
                               INNER JOIN wmbs_job ON
                                 wmbs_jobgroup.id = wmbs_job.jobgroup
                             WHERE wmbs_job.id = :jobid) AND fileid IN
                               (SELECT fileid FROM
                                  (SELECT wmbs_job_assoc.fileid AS fileid, wmbs_jobgroup.subscription AS subscription,
                                          COUNT(*) AS total_jobs, SUM(wmbs_job.outcome) AS successful_jobs
                                          FROM wmbs_job_assoc
                                     INNER JOIN
                                       (SELECT fileid FROM wmbs_job_assoc WHERE job = :jobid) job_input ON
                                       wmbs_job_assoc.fileid = job_input.fileid
                                     INNER JOIN wmbs_job ON
                                       wmbs_job_assoc.job = wmbs_job.id
                                     INNER JOIN wmbs_jobgroup ON
                                       wmbs_job.jobgroup = wmbs_jobgroup.id
                                   WHERE wmbs_jobgroup.subscription =
                                     (SELECT subscription FROM wmbs_jobgroup
                                        INNER JOIN wmbs_job ON
                                          wmbs_jobgroup.id = wmbs_job.jobgroup
                                      WHERE wmbs_job.id = :jobid)
                                   GROUP BY wmbs_job_assoc.fileid, wmbs_jobgroup.subscription)
                                WHERE total_jobs = successful_jobs)"""
                                 
    sql = """INSERT INTO wmbs_sub_files_complete (fileid, subscription)
               SELECT fileid, subscription FROM
                 (SELECT wmbs_job_assoc.fileid AS fileid, wmbs_jobgroup.subscription AS subscription,
                         COUNT(*) AS total_jobs, SUM(wmbs_job.outcome) AS successful_jobs
                         FROM wmbs_job_assoc
                    INNER JOIN
                      (SELECT fileid FROM wmbs_job_assoc WHERE job = :jobid) job_input ON
                      wmbs_job_assoc.fileid = job_input.fileid
                    INNER JOIN wmbs_job ON
                      wmbs_job_assoc.job = wmbs_job.id
                    INNER JOIN wmbs_jobgroup ON
                      wmbs_job.jobgroup = wmbs_jobgroup.id
                  WHERE wmbs_jobgroup.subscription =
                    (SELECT subscription FROM wmbs_jobgroup
                       INNER JOIN wmbs_job ON
                         wmbs_jobgroup.id = wmbs_job.jobgroup
                     WHERE wmbs_job.id = :jobid)
                  GROUP BY wmbs_job_assoc.fileid, wmbs_jobgroup.subscription)
               WHERE total_jobs = successful_jobs AND NOT EXISTS
                 (SELECT fileid FROM wmbs_sub_files_complete
                    WHERE subscription =
                      (SELECT subscription FROM wmbs_jobgroup
                         INNER JOIN wmbs_job ON
                           wmbs_jobgroup.id = wmbs_job.jobgroup
                       WHERE wmbs_job.id = :jobid) AND fileid IN
                      (SELECT fileid FROM wmbs_job_assoc
                       WHERE job = :jobid))"""
