#!/usr/bin/env python
"""
_ListIncomplete_

SQLite implementation of Subscription.ListIncomplete
"""

__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.ListIncomplete import ListIncomplete as ListIncompleteMySQL

class ListIncomplete(ListIncompleteMySQL):
    pass
