#!/usr/bin/env python
"""
_GetAllJobGroups_

Oracle implementation of Subscriptions.GetJobGroups
"""

__revision__ = "$Id: GetAllJobGroups.py,v 1.1 2010/01/14 16:16:39 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.GetAllJobGroups import GetAllJobGroups as MySQLGetAllJobGroups

class GetAllJobGroups(MySQLGetAllJobGroups):
    """
    Identical to MySQL version

    """
