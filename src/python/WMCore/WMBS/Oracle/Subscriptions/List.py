#!/usr/bin/env python
"""
_List_

Oracle implementation of Subscription.List
"""

__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.List import List as ListMySQL

class List(ListMySQL):
    sql =  ListMySQL.sql
