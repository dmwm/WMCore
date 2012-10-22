#!/usr/bin/env python
"""
_GetValidation_

MySQL implementation of Subscription.GetValidation
"""

from WMCore.Database.DBFormatter import DBFormatter

class GetValidation(DBFormatter):
    sql = """SELECT wmbs_location.site_name, wmbs_subscription_validation.valid
                    FROM wmbs_location
               INNER JOIN wmbs_subscription_validation ON
                 wmbs_location.id = wmbs_subscription_validation.location_id AND
                 wmbs_subscription_validation.subscription_id = :id"""

    def execute(self, id, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, {"id": id},
                                       transaction = transaction)
        formattedResults = self.formatDict(results)
        for formattedResult in formattedResults:
            if formattedResult["valid"] == 1:
                formattedResult = True
            else:
                formattedResult = False

        return formattedResults
