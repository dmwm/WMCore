#!/usr/bin/env python
"""
_GetJobGroups_

Oracle implementation of Subscription.GetJobGroups
"""

__revision__ = "$Id: GetJobGroups.py,v 1.2 2009/08/03 19:49:45 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Subscriptions.GetJobGroups import GetJobGroups as GetJobGroupsMySQL

class GetJobGroups(GetJobGroupsMySQL):    
    pass
