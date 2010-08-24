#!/usr/bin/env python
"""
_GetNumberOfJobsPerSite_

Oracle implementation of Jobs.GetNumberOfJobsPerSite
"""

__all__ = []



import logging

from WMCore.WMBS.MySQL.Jobs.GetNumberOfJobsPerSite import GetNumberOfJobsPerSite as MySQLGetNumberOfJobsPerSite

class GetNumberOfJobsPerSite(MySQLGetNumberOfJobsPerSite):
    """
    Identical to MySQL version

    """
