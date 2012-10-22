#!/usr/bin/env python
"""
_AddValidation_

MySQL implementation of Subscription.AddValidation
"""

from WMCore.Database.DBFormatter import DBFormatter

class AddValidation(DBFormatter):
    sql = """INSERT IGNORE INTO wmbs_subscription_validation
                               (subscription_id, location_id, valid)
               SELECT :sub, id, :valid FROM wmbs_location
               WHERE site_name = :site_name"""

    def execute(self, sites, conn = None, transaction = False):
        self.dbi.processData(self.sql, sites, conn = conn,
                             transaction = transaction)
        return
