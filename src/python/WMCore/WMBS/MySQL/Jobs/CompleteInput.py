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
                    HAVING total = numsuccess"""

    acquiredDelete = """DELETE FROM wmbs_sub_files_acquired
                        WHERE subscription = :subid AND fileid = :fileid"""

    failedDelete = """DELETE FROM wmbs_sub_files_failed
                      WHERE subscription = :subid AND fileid = :fileid"""

    sql = """INSERT IGNORE INTO wmbs_sub_files_complete (fileid, subscription)
               VALUES (:fileid, :subid)
               """

    failSql = """INSERT IGNORE INTO wmbs_sub_files_failed (fileid, subscription)
               VALUES (:fileid, :subid)
               """

    def execute(self, id, lfnsToSkip = None, conn = None, transaction = False):
        if isinstance(id, list):
            binds = []
            for singleID in id:
                binds.append({"jobid": singleID})
        else:
            binds = {"jobid": id}

        result = self.dbi.processData(self.fileSelect, binds, conn = conn,
                                      transaction = transaction)
        possibleDeletes = self.formatDict(result)

        completeBinds = []
        failBinds = []
        for possibleDelete in possibleDeletes:
            if lfnsToSkip and possibleDelete["lfn"] in lfnsToSkip.get(possibleDelete["jobid"], []):
                failBinds.append({"fileid": possibleDelete["fileid"],
                                  "subid": possibleDelete["subscriptionid"]})
            else:
                completeBinds.append({"fileid": possibleDelete["fileid"],
                                      "subid": possibleDelete["subscriptionid"]})

        if len(completeBinds) > 0:
            self.dbi.processData(self.acquiredDelete, completeBinds, conn = conn,
                                 transaction = transaction)
            self.dbi.processData(self.failedDelete, completeBinds, conn = conn,
                                 transaction = transaction)
            self.dbi.processData(self.sql, completeBinds, conn = conn,
                                 transaction = transaction)
        if len(failBinds) > 0:
            self.dbi.processData(self.acquiredDelete, failBinds, conn = conn,
                                 transaction = transaction)
            self.dbi.processData(self.failSql, failBinds, conn = conn,
                                 transaction = transaction)
        return
