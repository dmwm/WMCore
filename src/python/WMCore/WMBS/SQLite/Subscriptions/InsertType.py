#!/usr/bin/env python
"""
_InsertType_

SQLite implementation of Subscription.InsertType
"""




from WMCore.WMBS.MySQL.Subscriptions.InsertType import InsertType as InsertTypeMySQL

class InsertType(InsertTypeMySQL):
    sql = """INSERT INTO wmbs_sub_types (name)
               SELECT :name AS name WHERE NOT EXISTS
                (SELECT name FROM wmbs_sub_types WHERE name = :name)"""
