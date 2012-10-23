#!/usr/bin/env python
"""
_FailInput_

MySQL implementation of Jobs.FailInput
"""




from WMCore.Database.DBFormatter import DBFormatter

class FailInput(DBFormatter):
    """
    _FailInput_

    Given a job ID mark all it's input as failed for the subscription that
    produced it.  This will clear out any reference to the input files in the
    wmbs_sub_files_acquired and wmbs_sub_files_complete tables.
    """
    fileSelect = """SELECT wmbs_jobgroup.subscription AS subid,
                           wmbs_job_assoc.fileid AS fileid FROM wmbs_job_assoc
                      INNER JOIN wmbs_job ON
                        wmbs_job_assoc.job = wmbs_job.id
                      INNER JOIN wmbs_jobgroup ON
                        wmbs_job.jobgroup = wmbs_jobgroup.id
                      LEFT OUTER JOIN wmbs_sub_files_failed ON
                        wmbs_jobgroup.subscription = wmbs_sub_files_failed.subscription AND
                        wmbs_job_assoc.fileid = wmbs_sub_files_failed.fileid
                    WHERE wmbs_job.id = :jobid AND wmbs_sub_files_failed.fileid IS Null"""

    acquiredDelete = """DELETE FROM wmbs_sub_files_acquired
                        WHERE subscription = :subid AND fileid = :fileid"""

    completeDelete = """DELETE FROM wmbs_sub_files_complete
                      WHERE subscription = :subid AND fileid = :fileid"""

    sql = """INSERT IGNORE INTO wmbs_sub_files_failed (fileid, subscription)
               VALUES (:fileid, :subid)"""

    def execute(self, id, conn = None, transaction = False):
        results = self.dbi.processData(self.fileSelect, {"jobid": id}, conn = conn,
                                       transaction = transaction)
        delBinds = self.formatDict(results)

        if len(delBinds) > 0:
            self.dbi.processData(self.acquiredDelete, delBinds, conn = conn,
                                 transaction = transaction)
            self.dbi.processData(self.completeDelete, delBinds, conn = conn,
                                 transaction = transaction)
            self.dbi.processData(self.sql, delBinds, conn = conn,
                                 transaction = transaction)
        return
