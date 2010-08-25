#!/usr/bin/env python
"""
_CompleteInput_

Oracle implementation of Jobs.Complete
"""

__revision__ = "$Id: CompleteInput.py,v 1.2 2010/04/28 16:28:38 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Jobs.CompleteInput import CompleteInput as MySQLCompleteInput

class CompleteInput(MySQLCompleteInput):
    fileSelect = """SELECT job_files.subscriptionid, job_files.fileid,
                           COUNT(wmbs_job_assoc.job) AS total, SUM(wmbs_job.outcome) AS success FROM
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
                    GROUP BY job_files.subscriptionid, job_files.fileid"""    

    acquiredDelete = """DELETE FROM wmbs_sub_files_acquired
                        WHERE subscription = :subid AND fileid = :fileid"""

    failedDelete = """DELETE FROM wmbs_sub_files_failed
                      WHERE subscription = :subid AND fileid = :fileid"""    

    sql = """INSERT INTO wmbs_sub_files_complete (fileid, subscription)
               VALUES (:fileid, :subid)"""
