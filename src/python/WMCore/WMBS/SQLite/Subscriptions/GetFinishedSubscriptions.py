#!/usr/bin/env python
"""
_GetFinishedSubscriptions_

SQLiteimplementation of Subscription.GetFinishedSubscriptions
"""




from WMCore.WMBS.MySQL.Subscriptions.GetFinishedSubscriptions import GetFinishedSubscriptions as MySQLFinishedSubscriptions

class GetFinishedSubscriptions(MySQLFinishedSubscriptions):
    pass
