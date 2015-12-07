#!/usr/bin/env python
"""
_LoadFromID_

MySQL implementation of Subscription.LoadFromID
"""




from WMCore.Database.DBFormatter import DBFormatter

class LoadFromID(DBFormatter):
    sql = """SELECT wmbs_subscription.id, fileset, workflow, split_algo,
                    wmbs_sub_types.name, last_update FROM wmbs_subscription
               INNER JOIN wmbs_sub_types ON
                 wmbs_subscription.subtype = wmbs_sub_types.id
             WHERE wmbs_subscription.id = :id"""

    def formatDict(self, result):
        """
        _formatDict_

        Cast the id, fileset, workflow and last_update columns to integers
        since formatDict() turns everything into strings.
        """
        formattedResult = DBFormatter.formatDict(self, result)[0]
        formattedResult["id"] = int(formattedResult["id"])
        formattedResult["fileset"] = int(formattedResult["fileset"])
        formattedResult["workflow"] = int(formattedResult["workflow"])
        formattedResult["last_update"] = int(formattedResult["last_update"])
        formattedResult["type"] = formattedResult["name"]
        del formattedResult["name"]
        return formattedResult

    def execute(self, id = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"id": id}, conn = conn,
                                      transaction = transaction)
        return self.formatDict(result)
