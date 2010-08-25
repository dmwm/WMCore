#!/usr/bin/env python
"""
_Status_

SQLite implementation of JobGroup.Status
"""

__all__ = []
__revision__ = "$Id: Status.py,v 1.1 2009/04/29 16:27:01 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.Status import Status as StatusMySQL

class Status(StatusMySQL):
    sql = """select (
        select count(job) from wmbs_group_job_acquired where job=:job
        ) as ac, (
        select count(job) from wmbs_group_job_failed where job=:job
        ) as fa, (
        select count(job) from wmbs_group_job_complete where job=:job
        ) as cm 
    """
