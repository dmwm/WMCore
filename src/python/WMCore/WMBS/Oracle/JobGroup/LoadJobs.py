#!/usr/bin/env python
"""
_LoadJobs_

MySQL implementation of JobGroup.LoadJobs
"""

__all__ = []
__revision__ = "$Id: LoadJobs.py,v 1.1 2008/11/24 21:51:44 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.JobGroup.LoadJobs import LoadJobs as LoadJobsJobGroupMySQL

class LoadJobs(LoadJobsJobGroupMySQL):
    sql = LoadJobsJobGroupMySQL.sql