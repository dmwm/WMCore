#!/usr/bin/env python
"""
_FailInput_

Oracle implementation of Jobs.FailInput
"""

__revision__ = "$Id: FailInput.py,v 1.2 2010/04/28 20:43:26 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

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
                    WHERE wmbs_job.id = :jobid"""

    acquiredDelete = """DELETE FROM wmbs_sub_files_acquired
                        WHERE subscription = :subid AND fileid = :fileid"""

    completeDelete = """DELETE FROM wmbs_sub_files_complete
                      WHERE subscription = :subid AND fileid = :fileid"""    

    sql = """INSERT INTO wmbs_sub_files_failed (fileid, subscription)
               VALUES (:fileid, :subid)"""
