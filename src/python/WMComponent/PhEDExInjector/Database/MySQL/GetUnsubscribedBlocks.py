#!/usr/bin/env python

"""
_GetUnsubscribedBlocks_

MySQL implementation of PhEDExInjector.Database.GetUnsubscribedBlocks

Created on May 6, 2013

@author: dballest
"""

from WMCore.Database.DBFormatter import DBFormatter

class GetUnsubscribedBlocks(DBFormatter):
    """
    _GetUnsubscribedBlocks_

    Gets the unsubscribed closed blocks from DBSBuffer
    that are subscribed to a specific site and where
    the workflow that produced all the files in them
    is finished.
    """

    sql = """SELECT DISTINCT dbsbuffer_dataset_subscription.id,
                             dbsbuffer_dataset.path,
                             dbsbuffer_dataset_subscription.site,
                             dbsbuffer_block.blockname
               FROM dbsbuffer_dataset_subscription
               INNER JOIN dbsbuffer_dataset ON
                   dbsbuffer_dataset.id = dbsbuffer_dataset_subscription.dataset_id
               INNER JOIN dbsbuffer_algo_dataset_assoc ON
                 dbsbuffer_dataset.id = dbsbuffer_algo_dataset_assoc.dataset_id
               INNER JOIN dbsbuffer_file ON
                 dbsbuffer_algo_dataset_assoc.id = dbsbuffer_file.dataset_algo
               INNER JOIN dbsbuffer_block ON
                 dbsbuffer_file.block_id = dbsbuffer_block.id
               INNER JOIN dbsbuffer_workflow ON
                 dbsbuffer_file.workflow = dbsbuffer_workflow.id
               LEFT OUTER JOIN wmbs_workflow ON
                 wmbs_workflow.name = dbsbuffer_workflow.name
             WHERE dbsbuffer_file.status = 'GLOBAL' AND
                   dbsbuffer_file.in_phedex = 1 AND
                   dbsbuffer_dataset.path != 'bogus' AND
                   wmbs_workflow.id is NULL AND
                   dbsbuffer_block.status = 'Closed' AND
                   dbsbuffer_dataset_subscription.site = :node"""

    def execute(self, node = 'T0_CH_CERN',
                conn = None, transaction = False):
        binds = {'node' : node}
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return self.formatDict(result)
