#!/usr/bin/env python
"""
_GetFinishedSubscriptions_

SQLiteimplementation of Subscription.GetFinishedSubscriptions
"""

__revision__ = "$Id: GetFinishedSubscriptions.py,v 1.2 2010/08/05 20:41:42 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Subscriptions.GetFinishedSubscriptions import GetFinishedSubscriptions as MySQLFinishedSubscriptions

class GetFinishedSubscriptions(MySQLFinishedSubscriptions):
    pass
