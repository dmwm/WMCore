#!/usr/bin/env python
"""
_GetLocationsForJobs_

Oracle implementation of JobGroup.GetLocationsForJobs
"""

from WMCore.WMBS.MySQL.JobGroup.GetLocationsForJobs import GetLocationsForJobs as MySQLGetLocationsForJobs

class GetLocationsForJobs(MySQLGetLocationsForJobs):
    pass
