#!/usr/bin/env python
"""
_MarkNewFinishedSubscriptions_

Oracle implementation of Subscription.MarkNewFinishedSubscriptions
"""

from WMCore.WMBS.MySQL.Subscriptions.MarkNewFinishedSubscriptions \
        import MarkNewFinishedSubscriptions as MySQLMarkNewFinishedSubscriptions

class MarkNewFinishedSubscriptions(MySQLMarkNewFinishedSubscriptions):
    pass
