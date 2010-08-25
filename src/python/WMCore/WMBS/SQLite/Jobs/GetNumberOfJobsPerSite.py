#!/usr/bin/env python
"""
_GetNumberOfJobsPerSite_

SQLite implementation of Jobs.GetNumberOfJobsPerSite
"""

__all__ = []
__revision__ = "$Id: GetNumberOfJobsPerSite.py,v 1.1 2009/09/10 15:41:07 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import logging

from WMCore.WMBS.MySQL.Jobs.GetNumberOfJobsPerSite import GetNumberOfJobsPerSite as MySQLGetNumberOfJobsPerSite

class GetNumberOfJobsPerSite(MySQLGetNumberOfJobsPerSite):
    """
    Identical to MySQL version

    """
