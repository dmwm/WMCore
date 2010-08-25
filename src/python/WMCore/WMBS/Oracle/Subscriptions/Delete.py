#!/usr/bin/env python
"""
_Delete_

Oracle implementation of Subscription.Delete
"""

__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.Delete import Delete as DeleteMySQL

class Delete(DeleteMySQL):
    sql = DeleteMySQL.sql
