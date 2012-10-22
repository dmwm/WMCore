#!/usr/bin/env python
"""
_GetNumberOfJobsPerSite_

Oracle implementation of Jobs.GetNumberOfJobsPerSite
"""

__all__ = []



import logging

from WMCore.WMBS.MySQL.Subscriptions.GetNumberOfJobsPerSite import GetNumberOfJobsPerSite as MySQLGNOJPS

class GetNumberOfJobsPerSite(MySQLGNOJPS):
    """
    Right now the same as the MySQL implementation

    """
