#!/usr/bin/env python
"""
_GetNumberOfJobsPerSite_

Oracle implementation of Jobs.GetNumberOfJobsPerSite
"""

__all__ = []
__revision__ = "$Id: GetNumberOfJobsPerSite.py,v 1.1 2009/07/09 21:33:28 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import logging

from WMCore.WMBS.MySQL.Subscriptions.GetNumberOfJobsPerSite import GetNumberOfJobsPerSite as MySQLGNOJPS

class GetNumberOfJobsPerSite(MySQLGNOJPS):
    """
    Right now the same as the MySQL implementation

    """
    

    
