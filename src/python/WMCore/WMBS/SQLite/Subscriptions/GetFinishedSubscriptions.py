#!/usr/bin/env python
"""
_GetFinishedSubscriptions_

SQLiteimplementation of Subscription.GetFinishedSubscriptions
"""

__all__ = []
__revision__ = "$Id: GetFinishedSubscriptions.py,v 1.1 2009/12/14 22:25:46 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.GetFinishedSubscriptions import GetFinishedSubscriptions as MySQLFinishedSubscriptions

class GetFinishedSubscriptions(MySQLFinishedSubscriptions):
    """

    Identical to MySQL version
    """
