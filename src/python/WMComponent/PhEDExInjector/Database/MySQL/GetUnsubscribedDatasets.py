#!/usr/bin/env python
"""
_GetUnsubscribedDatasets_

MySQL implementation of PhEDExInjector.Database.GetUnsubscribedDatasets
"""

from WMCore.Database.DBFormatter import DBFormatter

class GetUnsubscribedDatasets(DBFormatter):
    """
    _GetUnsubscribedDatasets_

    Gets the unsubscribed datasets from DBSBuffer
    """

    sql = """SELECT DISTINCT dbsbuffer_dataset.path, dbsbuffer_workflow.name AS workflow,
                             dbsbuffer_workflow.spec AS spec
               FROM dbsbuffer_dataset
               INNER JOIN dbsbuffer_algo_dataset_assoc ON
                 dbsbuffer_dataset.id = dbsbuffer_algo_dataset_assoc.dataset_id
               INNER JOIN dbsbuffer_file ON
                 dbsbuffer_algo_dataset_assoc.id = dbsbuffer_file.dataset_algo
               LEFT OUTER JOIN dbsbuffer_workflow ON
                 dbsbuffer_file.workflow = dbsbuffer_workflow.id
             WHERE dbsbuffer_dataset.subscribed = 0 AND
                   dbsbuffer_file.status = 'GLOBAL' AND
                   dbsbuffer_file.in_phedex = 1 AND
                   dbsbuffer_dataset.path != 'bogus'"""

    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, conn = conn,
                                      transaction = transaction)
        return self.formatDict(result)
