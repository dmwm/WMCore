#!/usr/bin/env python
"""
_Status_

SQLite implementation of JobGroup.Status
"""

__all__ = []
__revision__ = "$Id: Status.py,v 1.4 2009/04/23 00:03:21 sryu Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMBS.MySQL.JobGroup.Status import Status as StatusMySQL

class Status(StatusMySQL):
    sql = """select (
        select count(id) from wmbs_job wj
            left outer join wmbs_group_job_acquired wa on wj.id = wa.job
            left outer join wmbs_group_job_failed wf on wj.id = wf.job
            left outer join wmbs_group_job_complete wc on wj.id = wc.job
            where jobgroup=:jobgroup and wa.job is null
                  and wf.job is null and wc.job is null
        ) as av, (
        select count(job) from wmbs_group_job_acquired where jobgroup=:jobgroup
        ) as ac, (
        select count(job) from wmbs_group_job_failed where jobgroup=:jobgroup
        ) as fa, (
        select count(job) from wmbs_group_job_complete where jobgroup=:jobgroup
        ) as cm 
    """
