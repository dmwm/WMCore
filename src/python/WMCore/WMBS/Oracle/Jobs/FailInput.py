#!/usr/bin/env python
"""
_FailInput_

Oracle implementation of Jobs.FailInput
"""




from WMCore.WMBS.MySQL.Jobs.FailInput import FailInput as MySQLFailInput

class FailInput(MySQLFailInput):
    """
    _FailInput_

    The file columns names are different in Oracle, we need to take that into
    account in these queries.
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

    sql = """INSERT INTO wmbs_sub_files_failed (fileid, subscription)
               SELECT :fileid, :subid FROM dual
               WHERE NOT EXISTS (SELECT fileid FROM wmbs_sub_files_failed wff2
                                   WHERE wff2.fileid = :fileid
                                   AND wff2.subscription = :subid)"""
