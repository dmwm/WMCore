#!/usr/bin/env python
"""
_FailInput_

MySQL implementation of Jobs.FailInput
"""

__revision__ = "$Id: FailInput.py,v 1.1 2009/10/13 20:04:10 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class FailInput(DBFormatter):
    """
    _FailInput_

    Given a job ID mark all it's input as failed for the subscription that
    produced it.  This will clear out any reference to the input files in the
    wmbs_sub_files_acquired and wmbs_sub_files_complete tables.
    """
    acquiredDelete = """DELETE FROM wmbs_sub_files_acquired
                          WHERE subscription =
                            (SELECT wmbs_subscription.id FROM wmbs_subscription
                               INNER JOIN wmbs_jobgroup ON
                                 wmbs_subscription.id = wmbs_jobgroup.subscription
                               INNER JOIN wmbs_job ON
                                 wmbs_jobgroup.id = wmbs_job.jobgroup
                             WHERE wmbs_job.id = :jobid) AND file IN
                             (SELECT file FROM wmbs_job_assoc
                                WHERE job = :jobid)"""

    completeDelete = """DELETE FROM wmbs_sub_files_complete
                          WHERE subscription =
                            (SELECT wmbs_subscription.id FROM wmbs_subscription
                               INNER JOIN wmbs_jobgroup ON
                                 wmbs_subscription.id = wmbs_jobgroup.subscription
                               INNER JOIN wmbs_job ON
                                 wmbs_jobgroup.id = wmbs_job.jobgroup
                             WHERE wmbs_job.id = :jobid) AND file IN
                             (SELECT file FROM wmbs_job_assoc
                                WHERE job = :jobid)"""    
                                 
    sql = """INSERT INTO wmbs_sub_files_failed (file, subscription)
               SELECT file, wmbs_jobgroup.subscription AS subscription
                      FROM wmbs_job_assoc
                 INNER JOIN wmbs_job ON
                   wmbs_job_assoc.job = wmbs_job.id
                 INNER JOIN wmbs_jobgroup ON
                   wmbs_job.jobgroup = wmbs_jobgroup.id
               WHERE wmbs_job_assoc.job = :jobid"""

    def execute(self, jobID, conn = None, transaction = False):
        self.dbi.processData(self.acquiredDelete, {"jobid": jobID}, conn = conn,
                             transaction = transaction)
        self.dbi.processData(self.completeDelete, {"jobid": jobID}, conn = conn,
                             transaction = transaction)
        self.dbi.processData(self.sql, {"jobid": jobID}, conn = conn,
                             transaction = transaction)        
        return
