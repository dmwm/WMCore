#!/usr/bin/env python
"""
_GetPartiallySubscribedDatasets_

MySQL implementation of PhEDExInjector.Database.GetPartiallySubscribedDatasets

Created on Oct 12, 2012

@author: dballest
"""

from WMCore.Database.DBFormatter import DBFormatter

class GetPartiallySubscribedDatasets(DBFormatter):
    """
    _GetPartiallySubscribedDatasets_

    Get the datasets which are in state 1 in the dbsbuffer table, and where
    the workflow in WMBS doesn't exist or is completed in all subscriptions
    that are not Merge, LogCollect or Cleanup
    """

    sql = """SELECT dbsbuffer_dataset.path, dbsbuffer_workflow.name AS workflow,
                    dbsbuffer_workflow.spec AS spec
               FROM dbsbuffer_dataset
               INNER JOIN dbsbuffer_algo_dataset_assoc ON
                 dbsbuffer_dataset.id = dbsbuffer_algo_dataset_assoc.dataset_id
               INNER JOIN dbsbuffer_file ON
                 dbsbuffer_algo_dataset_assoc.id = dbsbuffer_file.dataset_algo
               LEFT OUTER JOIN dbsbuffer_workflow ON
                 dbsbuffer_file.workflow = dbsbuffer_workflow.id
               LEFT OUTER JOIN wmbs_workflow ON
                 wmbs_workflow.name = dbsbuffer_workflow.name
               LEFT OUTER JOIN wmbs_subscription ON
                 wmbs_subscription.workflow = wmbs_workflow.id AND
                 wmbs_subscription.finished = 0
               LEFT OUTER JOIN wmbs_sub_types ON
                 wmbs_sub_types.id = wmbs_subscription.subtype AND
                 wmbs_sub_types.name NOT IN ('Cleanup', 'LogCollect', 'Merge')
             WHERE dbsbuffer_dataset.subscribed = 1 AND
                   dbsbuffer_file.status = 'GLOBAL' AND
                   dbsbuffer_file.in_phedex = 1 AND
                   dbsbuffer_dataset.path != 'bogus'
             GROUP BY dbsbuffer_dataset.path, dbsbuffer_workflow.name, dbsbuffer_workflow.spec
             HAVING COUNT(wmbs_sub_types.id) = 0 OR
                    COUNT(wmbs_workflow.id) = 0"""

    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, conn = conn,
                                      transaction = transaction)
        return self.formatDict(result)
