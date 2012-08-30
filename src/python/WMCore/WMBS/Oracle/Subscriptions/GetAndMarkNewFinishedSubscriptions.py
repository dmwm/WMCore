#!/usr/bin/env python
"""
_GetAndMarkNewFinishedSubscriptions_

Oracle implementation of Subscription.GetAndMarkNewFinishedSubscriptions
"""

from WMCore.WMBS.MySQL.Subscriptions.GetAndMarkNewFinishedSubscriptions \
        import GetAndMarkNewFinishedSubscriptions as MySQLGetAndMarkNewFinishedSubscriptions

class GetAndMarkNewFinishedSubscriptions(MySQLGetAndMarkNewFinishedSubscriptions):
    pass
