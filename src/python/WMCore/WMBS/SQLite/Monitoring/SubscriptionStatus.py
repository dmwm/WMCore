#!/usr/bin/env python
"""
_SubscriptionStatus_

SQLite implementation of Monitoring.SubscriptionStatus
"""

__revision__ = "$Id: SubscriptionStatus.py,v 1.1 2009/11/17 18:33:47 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Monitoring.SubscriptionStatus import SubscriptionStatus \
 as SubscriptionStatusMySQL

class SubscriptionStatus(SubscriptionStatusMySQL):
    pass
