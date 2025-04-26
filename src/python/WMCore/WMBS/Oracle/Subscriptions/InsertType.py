#!/usr/bin/env python
"""
_InsertType_

Oracle implementation of Subscription.InsertType
"""




from WMCore.WMBS.MySQL.Subscriptions.InsertType import InsertType as InsertTypeMySQL

class InsertType(InsertTypeMySQL):
    sql = """INSERT INTO wmbs_sub_types (name)
             SELECT :name AS name FROM DUAL WHERE NOT EXISTS
             (SELECT id FROM wmbs_sub_types WHERE name = :name)"""
