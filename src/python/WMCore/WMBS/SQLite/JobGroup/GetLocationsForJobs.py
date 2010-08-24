#!/usr/bin/env python
"""
_GetLocationsForJobs_

SQLite implementation of JobGroup.GetLocationsForJobs
"""

__all__ = []



import logging
import inspect

from WMCore.WMBS.MySQL.JobGroup.GetLocationsForJobs import GetLocationsForJobs as MySQLGetLocationsForJobs

class GetLocationsForJobs(MySQLGetLocationsForJobs):
    """
    Identical to MySQL version
    """
