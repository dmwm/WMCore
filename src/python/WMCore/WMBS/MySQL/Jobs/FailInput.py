#!/usr/bin/env python
"""
_FailInput_

MySQL implementation of Jobs.FailInput
"""

__revision__ = "$Id: FailInput.py,v 1.3 2010/04/28 20:43:26 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

class FailInput(DBFormatter):
    """
    _FailInput_

    Given a job ID mark all it's input as failed for the subscription that
    produced it.  This will clear out any reference to the input files in the
    wmbs_sub_files_acquired and wmbs_sub_files_complete tables.
    """
    fileSelect = """SELECT wmbs_jobgroup.subscription AS subid,
                           wmbs_job_assoc.file AS fileid FROM wmbs_job_assoc
                      INNER JOIN wmbs_job ON
                        wmbs_job_assoc.job = wmbs_job.id
                      INNER JOIN wmbs_jobgroup ON
                        wmbs_job.jobgroup = wmbs_jobgroup.id
                    WHERE wmbs_job.id = :jobid"""

    acquiredDelete = """DELETE FROM wmbs_sub_files_acquired
                        WHERE subscription = :subid AND file = :fileid"""

    completeDelete = """DELETE FROM wmbs_sub_files_complete
                      WHERE subscription = :subid AND file = :fileid"""    

    sql = """INSERT INTO wmbs_sub_files_failed (file, subscription)
               VALUES (:fileid, :subid)"""

    def execute(self, id, conn = None, transaction = False):
        results = self.dbi.processData(self.fileSelect, {"jobid": id}, conn = conn,
                                       transaction = transaction)
        delBinds = self.formatDict(results)

        self.dbi.processData(self.acquiredDelete, delBinds, conn = conn,
                             transaction = transaction)        
        self.dbi.processData(self.completeDelete, delBinds, conn = conn,
                             transaction = transaction)
        self.dbi.processData(self.sql, delBinds, conn = conn,
                             transaction = transaction)        
        return
