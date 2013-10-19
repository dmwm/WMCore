#!/usr/bin/env python

"""
_GetSubscribedBlocks_

MySQL implementation of PhEDExInjector.Database.GetSubscribedBlocks

Created on May 6, 2013

@author: dballest
"""
import time

from WMCore.Database.DBFormatter import DBFormatter

class GetSubscribedBlocks(DBFormatter):
    """
    _GetSubscribedBlocks_

    Gets the subscribed closed blocks from DBSBuffer
    that are subscribed to a specific site and where
    the workflow that produced all the files in them
    is finished. It also allows a minimum creation date for the blocks.
    """

    sql = """SELECT dbsbuffer_dataset.path,
                    dbsbuffer_block.blockname,
                    dbsbuffer_file_location.location
             FROM dbsbuffer_dataset_subscription
             INNER JOIN dbsbuffer_dataset ON
               dbsbuffer_dataset.id = dbsbuffer_dataset_subscription.dataset_id AND
               dbsbuffer_dataset.path != 'bogus'
             INNER JOIN dbsbuffer_algo_dataset_assoc ON
               dbsbuffer_dataset.id = dbsbuffer_algo_dataset_assoc.dataset_id
             INNER JOIN dbsbuffer_file ON
               dbsbuffer_algo_dataset_assoc.id = dbsbuffer_file.dataset_algo AND
               dbsbuffer_file.status IN ( 'GLOBAL', 'InDBS' ) AND
               dbsbuffer_file.in_phedex = 1
             INNER JOIN dbsbuffer_file_location ON
               dbsbuffer_file_location.filename = dbsbuffer_file.id
             INNER JOIN dbsbuffer_block ON
               dbsbuffer_file.block_id = dbsbuffer_block.id AND
               dbsbuffer_block.status = 'Closed' AND
               dbsbuffer_block.create_time > :creationTime
             INNER JOIN dbsbuffer_workflow ON
               dbsbuffer_file.workflow = dbsbuffer_workflow.id
             LEFT OUTER JOIN wmbs_workflow ON
               wmbs_workflow.name = dbsbuffer_workflow.name
             WHERE dbsbuffer_dataset_subscription.subscribed = 1 AND
                   wmbs_workflow.id is NULL
             GROUP BY dbsbuffer_dataset.path,
                      dbsbuffer_block.blockname,
                      dbsbuffer_file_location.location"""

    def execute(self, timeout, conn = None, transaction = False):

        binds = { 'creationTime' : int(time.time()) - timeout }

        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)

        return self.formatDict(result)
