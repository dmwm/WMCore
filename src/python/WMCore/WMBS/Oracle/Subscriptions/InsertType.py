#!/usr/bin/env python
"""
_InsertType_

Oracle implementation of Subscription.InsertType
"""




from WMCore.WMBS.MySQL.Subscriptions.InsertType import InsertType as InsertTypeMySQL

class InsertType(InsertTypeMySQL):
    sql = """INSERT INTO wmbs_sub_types (id, name)
               SELECT wmbs_sub_types_SEQ.nextval, :name AS name FROM DUAL WHERE NOT EXISTS
                (SELECT name FROM wmbs_sub_types WHERE name = :name)"""
