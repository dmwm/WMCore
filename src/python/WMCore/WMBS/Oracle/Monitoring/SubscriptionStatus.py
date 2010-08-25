#!/usr/bin/env python
"""
_SubscriptionStatus_

Oracle implementation of Monitoring.SubscriptionStatus
"""




from WMCore.WMBS.MySQL.Monitoring.SubscriptionStatus import SubscriptionStatus \
 as SubscriptionStatusMySQL

class SubscriptionStatus(SubscriptionStatusMySQL):
    pass
