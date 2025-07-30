#!/usr/bin/env python
"""
_Subscription.New_

Oracle implementation of Subscription.New
"""

from WMCore.WMBS.MySQL.Subscriptions.New import New as NewMySQL

class New(NewMySQL):
    typesSQL = """INSERT INTO wmbs_sub_types (name)
                    SELECT :subtype FROM dual
                    WHERE NOT EXISTS (SELECT id FROM wmbs_sub_types WHERE name = :subtype)"""

    sql = """INSERT INTO wmbs_subscription
             (fileset, workflow, subtype, split_algo, last_update)
             SELECT :fileset, :workflow, id, :split_algo, :timestamp
             FROM wmbs_sub_types WHERE name = :subtype"""
