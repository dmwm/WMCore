#!/usr/bin/env python
"""
_CompleteInput_

MySQL implementation of Jobs.Complete
"""

__revision__ = "$Id: CompleteInput.py,v 1.2 2009/10/14 20:29:46 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class CompleteInput(DBFormatter):
    """
    _CompleteInput_

    Mark the input files for a job as complete if and only if all the jobs that
    run over the files have been complete successfully.  This will also remove
    entries from the acquired and failed tables.
    """
    acquiredDelete = """DELETE FROM wmbs_sub_files_acquired
                          WHERE subscription =
                            (SELECT wmbs_subscription.id FROM wmbs_subscription
                               INNER JOIN wmbs_jobgroup ON
                                 wmbs_subscription.id = wmbs_jobgroup.subscription
                               INNER JOIN wmbs_job ON
                                 wmbs_jobgroup.id = wmbs_job.jobgroup
                             WHERE wmbs_job.id = :jobid) AND file IN
                               (SELECT file FROM
                                  (SELECT wmbs_job_assoc.file AS file, wmbs_jobgroup.subscription AS subscription,
                                          COUNT(*) AS total_jobs, SUM(wmbs_job.outcome) AS successful_jobs
                                          FROM wmbs_job_assoc
                                     INNER JOIN
                                       (SELECT file FROM wmbs_job_assoc WHERE job = :jobid) job_input ON
                                       wmbs_job_assoc.file = job_input.file
                                     INNER JOIN wmbs_job ON
                                       wmbs_job_assoc.job = wmbs_job.id
                                     INNER JOIN wmbs_jobgroup ON
                                       wmbs_job.jobgroup = wmbs_jobgroup.id
                                   WHERE wmbs_jobgroup.subscription =
                                     (SELECT subscription FROM wmbs_jobgroup
                                        INNER JOIN wmbs_job ON
                                          wmbs_jobgroup.id = wmbs_job.jobgroup
                                      WHERE wmbs_job.id = :jobid)
                                   GROUP BY wmbs_job_assoc.file, wmbs_jobgroup.subscription) file_table
                                WHERE total_jobs = successful_jobs)"""

    failedDelete = """DELETE FROM wmbs_sub_files_failed
                          WHERE subscription =
                            (SELECT wmbs_subscription.id FROM wmbs_subscription
                               INNER JOIN wmbs_jobgroup ON
                                 wmbs_subscription.id = wmbs_jobgroup.subscription
                               INNER JOIN wmbs_job ON
                                 wmbs_jobgroup.id = wmbs_job.jobgroup
                             WHERE wmbs_job.id = :jobid) AND file IN
                               (SELECT file FROM
                                  (SELECT wmbs_job_assoc.file AS file, wmbs_jobgroup.subscription AS subscription,
                                          COUNT(*) AS total_jobs, SUM(wmbs_job.outcome) AS successful_jobs
                                          FROM wmbs_job_assoc
                                     INNER JOIN
                                       (SELECT file FROM wmbs_job_assoc WHERE job = :jobid) job_input ON
                                       wmbs_job_assoc.file = job_input.file
                                     INNER JOIN wmbs_job ON
                                       wmbs_job_assoc.job = wmbs_job.id
                                     INNER JOIN wmbs_jobgroup ON
                                       wmbs_job.jobgroup = wmbs_jobgroup.id
                                   WHERE wmbs_jobgroup.subscription =
                                     (SELECT subscription FROM wmbs_jobgroup
                                        INNER JOIN wmbs_job ON
                                          wmbs_jobgroup.id = wmbs_job.jobgroup
                                      WHERE wmbs_job.id = :jobid)
                                   GROUP BY wmbs_job_assoc.file, wmbs_jobgroup.subscription) file_table
                                WHERE total_jobs = successful_jobs)"""
                                 
    sql = """INSERT INTO wmbs_sub_files_complete (file, subscription)
               SELECT file, subscription FROM
                 (SELECT wmbs_job_assoc.file AS file, wmbs_jobgroup.subscription AS subscription,
                         COUNT(*) AS total_jobs, SUM(wmbs_job.outcome) AS successful_jobs
                         FROM wmbs_job_assoc
                    INNER JOIN
                      (SELECT file FROM wmbs_job_assoc WHERE job = :jobid) job_input ON
                      wmbs_job_assoc.file = job_input.file
                    INNER JOIN wmbs_job ON
                      wmbs_job_assoc.job = wmbs_job.id
                    INNER JOIN wmbs_jobgroup ON
                      wmbs_job.jobgroup = wmbs_jobgroup.id
                  WHERE wmbs_jobgroup.subscription =
                    (SELECT subscription FROM wmbs_jobgroup
                       INNER JOIN wmbs_job ON
                         wmbs_jobgroup.id = wmbs_job.jobgroup
                     WHERE wmbs_job.id = :jobid)
                  GROUP BY wmbs_job_assoc.file, wmbs_jobgroup.subscription) file_table
               WHERE total_jobs = successful_jobs AND NOT EXISTS
                 (SELECT file FROM wmbs_sub_files_complete
                    WHERE subscription =
                      (SELECT subscription FROM wmbs_jobgroup
                         INNER JOIN wmbs_job ON
                           wmbs_jobgroup.id = wmbs_job.jobgroup
                       WHERE wmbs_job.id = :jobid) AND file IN
                      (SELECT file FROM wmbs_job_assoc
                       WHERE job = :jobid))"""
    
    def execute(self, jobID, conn = None, transaction = False):
        self.dbi.processData(self.acquiredDelete, {"jobid": jobID}, conn = conn,
                             transaction = transaction)
        self.dbi.processData(self.failedDelete, {"jobid": jobID}, conn = conn,
                             transaction = transaction)
        self.dbi.processData(self.sql, {"jobid": jobID}, conn = conn,
                             transaction = transaction)        
        return
