#!/usr/bin/env python
"""
_LoadFromID_

SQLite implementation of Subscription.LoadFromID
"""

__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.LoadFromID import LoadFromID as LoadFromIDMySQL

class LoadFromID(LoadFromIDMySQL):
    sql = LoadFromIDMySQL.sql
