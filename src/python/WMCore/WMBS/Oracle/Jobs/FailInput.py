#!/usr/bin/env python
"""
_FailInput_

Oracle implementation of Jobs.FailInput
"""

__revision__ = "$Id: FailInput.py,v 1.1 2009/10/13 20:04:10 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.FailInput import FailInput as MySQLFailInput

class FailInput(MySQLFailInput):
    """
    _FailInput_

    The file columns names are different in Oracle, we need to take that into
    account in these queries.
    """
    acquiredDelete = """DELETE FROM wmbs_sub_files_acquired
                          WHERE subscription =
                            (SELECT wmbs_subscription.id FROM wmbs_subscription
                               INNER JOIN wmbs_jobgroup ON
                                 wmbs_subscription.id = wmbs_jobgroup.subscription
                               INNER JOIN wmbs_job ON
                                 wmbs_jobgroup.id = wmbs_job.jobgroup
                             WHERE wmbs_job.id = :jobid) AND fileid IN
                             (SELECT fileid FROM wmbs_job_assoc
                                WHERE job = :jobid)"""

    completeDelete = """DELETE FROM wmbs_sub_files_complete
                          WHERE subscription =
                            (SELECT wmbs_subscription.id FROM wmbs_subscription
                               INNER JOIN wmbs_jobgroup ON
                                 wmbs_subscription.id = wmbs_jobgroup.subscription
                               INNER JOIN wmbs_job ON
                                 wmbs_jobgroup.id = wmbs_job.jobgroup
                             WHERE wmbs_job.id = :jobid) AND fileid IN
                             (SELECT fileid FROM wmbs_job_assoc
                                WHERE job = :jobid)"""    
                                 
    sql = """INSERT INTO wmbs_sub_files_failed (fileid, subscription)
               SELECT fileid, wmbs_jobgroup.subscription AS subscription
                      FROM wmbs_job_assoc
                 INNER JOIN wmbs_job ON
                   wmbs_job_assoc.job = wmbs_job.id
                 INNER JOIN wmbs_jobgroup ON
                   wmbs_job.jobgroup = wmbs_jobgroup.id
               WHERE wmbs_job_assoc.job = :jobid AND NOT EXISTS
                 (SELECT fileid FROM wmbs_sub_files_failed
                    WHERE subscription =
                      (SELECT subscription FROM wmbs_jobgroup
                         INNER JOIN wmbs_job ON
                           wmbs_jobgroup.id = wmbs_job.jobgroup
                       WHERE wmbs_job.id = :jobid) AND fileid IN
                      (SELECT fileid FROM wmbs_job_assoc
                       WHERE job = :jobid))"""
