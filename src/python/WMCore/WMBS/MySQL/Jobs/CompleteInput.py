#!/usr/bin/env python
"""
_CompleteInput_

MySQL implementation of Jobs.Complete
"""

from WMCore.Database.DBFormatter import DBFormatter

class CompleteInput(DBFormatter):
    """
    _CompleteInput_

    Mark the input files for a job as complete if and only if all the jobs that
    run over the files have been complete successfully.  This will also remove
    entries from the acquired and failed tables.
    """
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
                    HAVING total = numsuccess"""

    acquiredDelete = """DELETE FROM wmbs_sub_files_acquired
                        WHERE subscription = :subid AND fileid = :fileid"""

    failedDelete = """DELETE FROM wmbs_sub_files_failed
                      WHERE subscription = :subid AND fileid = :fileid"""

    sql = """INSERT IGNORE INTO wmbs_sub_files_complete (fileid, subscription)
               VALUES (:fileid, :subid)
               """

    def execute(self, id, conn = None, transaction = False):
        if type(id) == list:
            binds = []
            for singleID in id:
                binds.append({"jobid": singleID})
        else:
            binds = {"jobid": id}

        result = self.dbi.processData(self.fileSelect, binds, conn = conn,
                                      transaction = transaction)
        possibleDeletes = self.formatDict(result)

        binds = []
        for possibleDelete in possibleDeletes:
            binds.append({"fileid": possibleDelete["fileid"],
                          "subid": possibleDelete["subscriptionid"]})

        if len(binds) == 0:
            return

        self.dbi.processData(self.acquiredDelete, binds, conn = conn,
                             transaction = transaction)
        self.dbi.processData(self.failedDelete, binds, conn = conn,
                             transaction = transaction)
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
