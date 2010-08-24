#!/usr/bin/env python
"""
_LoadJobs_

SQLite implementation of JobGroup.LoadJobs
"""

__all__ = []
__revision__ = "$Id: LoadJobs.py,v 1.1 2008/11/21 17:14:58 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.JobGroup.LoadJobs import LoadJobs as LoadJobsMySQL

class LoadJobs(LoadJobsMySQL):
    sql = LoadJobsMySQL.sql
