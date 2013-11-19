#!/usr/bin/env python

"""
_GetSubscribedBlocks_

MySQL implementation of PhEDExInjector.Database.GetSubscribedBlocks

Created on May 6, 2013

@author: dballest
"""

from WMCore.Database.DBFormatter import DBFormatter

class GetSubscribedBlocks(DBFormatter):
    """
    _GetSubscribedBlocks_

    Gets the subscribed closed blocks from DBSBuffer
    that are subscribed to a specific site and where
    the workflow that produced all the files in them
    is finished. It also allows a minimum creation date for the blocks.
    """

    sql = """SELECT DISTINCT dbsbuffer_dataset.path, dbsbuffer_block.blockname
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
             WHERE wmbs_workflow.id is NULL AND
                   dbsbuffer_block.status = 'Closed' AND
                   dbsbuffer_dataset_subscription.site = :node AND
                   dbsbuffer_dataset_subscription.subscribed = 1 AND
                   dbsbuffer_block.create_time > :creationDate"""

    def execute(self, node = 'T0_CH_CERN', creationDate = -1,
                conn = None, transaction = False):
        binds = {'node' : node, 'creationDate' : creationDate}
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return self.formatDict(result)
