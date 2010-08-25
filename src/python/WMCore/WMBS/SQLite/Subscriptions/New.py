#!/usr/bin/env python
"""
_Subscription.New_

SQLite implementation of Subscription.New
"""




from WMCore.WMBS.MySQL.Subscriptions.New import New as NewMySQL

class New(NewMySQL):
    """
    Create a workflow ready for subscriptions
    """
    pass
