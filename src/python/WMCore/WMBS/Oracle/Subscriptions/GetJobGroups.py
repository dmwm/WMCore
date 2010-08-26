#!/usr/bin/env python
"""
_GetJobGroups_

Oracle implementation of Subscriptions.GetJobGroups
"""

__revision__ = "$Id: GetJobGroups.py,v 1.3 2009/10/28 12:51:43 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Subscriptions.GetJobGroups import GetJobGroups as GetJobGroupsMySQL

class GetJobGroups(GetJobGroupsMySQL):    
    pass
