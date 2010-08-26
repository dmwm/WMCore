#!/usr/bin/env python
"""
_LoadJobs_

Oracle implementation of JobGroup.LoadJobs
"""

__all__ = []
__revision__ = "$Id: LoadJobs.py,v 1.2 2008/12/05 21:06:26 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.JobGroup.LoadJobs import LoadJobs as LoadJobsJobGroupMySQL

class LoadJobs(LoadJobsJobGroupMySQL):
    sql = LoadJobsJobGroupMySQL.sql