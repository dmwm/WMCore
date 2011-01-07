#!/usr/bin/env python
"""
_AddValidation_

Oracle implementation of Subscription.AddValidation
"""

from WMCore.WMBS.MySQL.Subscriptions.AddValidation import AddValidation as AddValidationMySQL

class AddValidation(AddValidationMySQL):
    sql = """INSERT INTO wmbs_subscription_validation
                               (subscription_id, location_id, valid)
               SELECT :sub, id, :valid FROM wmbs_location
               WHERE site_name = :site_name AND NOT EXISTS
               (SELECT * FROM wmbs_subscription_validation
                WHERE subscription_id = :sub AND location_id = wmbs_location.id
                      AND valid = :valid)"""
