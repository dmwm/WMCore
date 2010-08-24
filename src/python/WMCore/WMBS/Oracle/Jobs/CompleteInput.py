#!/usr/bin/env python
"""
_CompleteInput_

Oracle implementation of Jobs.CompleteInput
"""

from WMCore.WMBS.MySQL.Jobs.CompleteInput import CompleteInput as MySQLCompleteInput

class CompleteInput(MySQLCompleteInput):
    fileSelect = """SELECT job_files.subscriptionid, job_files.fileid,
                           COUNT(wmbs_job_assoc.job) AS total, SUM(wmbs_job.outcome) AS numsuccess FROM
                      (SELECT wmbs_jobgroup.subscription AS subscriptionid,
                              wmbs_job_assoc.fileid AS fileid FROM wmbs_job_assoc
                         INNER JOIN wmbs_job ON
                           wmbs_job_assoc.job = wmbs_job.id
                         INNER JOIN wmbs_jobgroup ON
                           wmbs_job.jobgroup = wmbs_jobgroup.id
                       WHERE wmbs_job.id = :jobid) job_files
                      INNER JOIN wmbs_job_assoc ON
                        job_files.fileid = wmbs_job_assoc.fileid
                      INNER JOIN wmbs_job ON
                        wmbs_job_assoc.job = wmbs_job.id
                      INNER JOIN wmbs_jobgroup ON
                        wmbs_job.jobgroup = wmbs_jobgroup.id
                    WHERE wmbs_jobgroup.subscription = job_files.subscriptionid    
                    GROUP BY job_files.subscriptionid, job_files.fileid
                    HAVING COUNT(wmbs_job_assoc.job) = SUM(wmbs_job.outcome)"""

    acquiredDelete = """DELETE FROM wmbs_sub_files_acquired
                        WHERE subscription = :subid AND fileid = :fileid"""

    failedDelete = """DELETE FROM wmbs_sub_files_failed
                      WHERE subscription = :subid AND fileid = :fileid"""    

    sql = """INSERT INTO wmbs_sub_files_complete (fileid, subscription)
               SELECT :fileid, :subid FROM DUAL
               WHERE NOT EXISTS
                 (SELECT * FROM wmbs_sub_files_complete
                  WHERE fileid = :fileid AND subscription = :subid)"""    
