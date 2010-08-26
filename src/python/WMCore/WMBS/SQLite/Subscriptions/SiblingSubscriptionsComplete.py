#!/usr/bin/env python
"""
_SiblingSubscriptionsComplete_

SQLite implementation of Subscription.SiblingSubscriptionsComplete
"""

__revision__ = "$Id: SiblingSubscriptionsComplete.py,v 1.1 2010/04/22 15:42:40 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.SiblingSubscriptionsComplete import \
    SiblingSubscriptionsComplete as SiblingCompleteMySQL    

class SiblingSubscriptionsComplete(SiblingCompleteMySQL):
    pass

