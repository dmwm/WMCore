#!/usr/bin/env python
"""
_CompleteInput_

Oracle implementation of Jobs.CompleteInput
"""

from WMCore.WMBS.MySQL.Jobs.CompleteInput import CompleteInput as MySQLCompleteInput

class CompleteInput(MySQLCompleteInput):
    fileSelect = """SELECT job_files.subscriptionid, job_files.fileid, job_files.lfn,
                           job_files.jobid,
                           COUNT(wmbs_job_assoc.job) AS total, SUM(wmbs_job.outcome) AS numsuccess FROM
                      (SELECT wmbs_jobgroup.subscription AS subscriptionid,
                              wmbs_job_assoc.job AS jobid,
                              wmbs_job_assoc.fileid AS fileid,
                              wmbs_file_details.lfn AS lfn FROM wmbs_job_assoc
                         INNER JOIN wmbs_job ON
                           wmbs_job_assoc.job = wmbs_job.id
                         INNER JOIN wmbs_jobgroup ON
                           wmbs_job.jobgroup = wmbs_jobgroup.id
                         INNER JOIN wmbs_file_details ON
                           wmbs_job_assoc.fileid = wmbs_file_details.id
                       WHERE wmbs_job.id = :jobid) job_files
                      INNER JOIN wmbs_job_assoc ON
                        job_files.fileid = wmbs_job_assoc.fileid
                      INNER JOIN wmbs_job ON
                        wmbs_job_assoc.job = wmbs_job.id
                      INNER JOIN wmbs_jobgroup ON
                        wmbs_job.jobgroup = wmbs_jobgroup.id
                    WHERE wmbs_jobgroup.subscription = job_files.subscriptionid
                    GROUP BY job_files.subscriptionid, job_files.fileid, job_files.lfn,
                             job_files.jobid
                    HAVING COUNT(wmbs_job_assoc.job) = SUM(wmbs_job.outcome)"""

    sql = """INSERT INTO wmbs_sub_files_complete (fileid, subscription)
               SELECT :fileid, :subid FROM DUAL
               WHERE NOT EXISTS
                 (SELECT * FROM wmbs_sub_files_complete
                  WHERE fileid = :fileid AND subscription = :subid)"""

    failSql = """INSERT INTO wmbs_sub_files_failed (fileid, subscription)
                   SELECT :fileid, :subid FROM DUAL
                   WHERE NOT EXISTS
                     (SELECT * FROM wmbs_sub_files_failed
                      WHERE fileid = :fileid AND subscription = :subid)"""
