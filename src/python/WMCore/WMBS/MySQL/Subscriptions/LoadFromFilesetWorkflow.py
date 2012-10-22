#!/usr/bin/env python
"""
_LoadFromFilesetWorkflow_

MySQL implementation of Subscription.LoadFromFilesetWorkflow
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class LoadFromFilesetWorkflow(DBFormatter):
    sql = """SELECT wmbs_subscription.id, fileset, workflow, split_algo,
                    wmbs_sub_types.name, last_update FROM wmbs_subscription
               INNER JOIN wmbs_sub_types ON
                 wmbs_subscription.subtype = wmbs_sub_types.id
             WHERE fileset = :fileset AND workflow = :workflow"""

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

    def execute(self, fileset = None, workflow = None, conn = None,
                transaction = False):
        result = self.dbi.processData(self.sql, {"fileset": fileset,
                                                 "workflow": workflow},
                                      conn = conn, transaction = transaction)
        return self.formatDict(result)
