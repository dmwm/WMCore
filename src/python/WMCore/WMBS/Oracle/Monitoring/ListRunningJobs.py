#!/usr/bin/env python
"""
_ListRunningJobs_

Oracle implementation of Monitoring.ListRunningJobs
"""

__revision__ = "$Id: ListRunningJobs.py,v 1.1 2010/01/26 17:35:46 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Monitoring.ListRunningJobs import ListRunningJobs \
    as ListRunningJobsMySQL

class ListRunningJobs(ListRunningJobsMySQL):
    pass
