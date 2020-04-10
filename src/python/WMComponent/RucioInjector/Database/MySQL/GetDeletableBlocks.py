"""
_GetDeletableBlocks_

MySQL implementation of PhEDExInjector.GetDeletableBlocks

Retrieve a list of blocks that can be deleted,
including their location and the sites they
are subscribed too

"""

from __future__ import division
from __future__ import print_function

from WMCore.Database.DBFormatter import DBFormatter


class GetDeletableBlocks(DBFormatter):
    # Retrieve a list of blocks that can be deleted:
    #   - workflow for all files in block completed
    #   - subscription made for dataset is copy+delete
    #   - subscription has been made in PhEDEx
    #   - blocks hasn't been deleted yet

    # FIXME: returns SE, should switch to PNN soon

    sql = """SELECT dbsbuffer_block.blockname,
                    dbsbuffer_location.pnn,
                    dbsbuffer_dataset.path,
                    dbsbuffer_dataset_subscription.site
             FROM dbsbuffer_dataset_subscription
             INNER JOIN dbsbuffer_dataset ON
               dbsbuffer_dataset.id = dbsbuffer_dataset_subscription.dataset_id
             INNER JOIN dbsbuffer_block ON
               dbsbuffer_block.dataset_id = dbsbuffer_dataset_subscription.dataset_id
             INNER JOIN dbsbuffer_file ON
               dbsbuffer_file.block_id = dbsbuffer_block.id
             INNER JOIN dbsbuffer_workflow ON
               dbsbuffer_workflow.id = dbsbuffer_file.workflow
             INNER JOIN dbsbuffer_location ON
               dbsbuffer_location.id = dbsbuffer_block.location
             LEFT OUTER JOIN wmbs_workflow ON
               wmbs_workflow.name = dbsbuffer_workflow.name
             WHERE dbsbuffer_dataset_subscription.delete_blocks = 1
             AND dbsbuffer_dataset_subscription.subscribed = 1
             AND dbsbuffer_block.status = 'Closed'
             AND dbsbuffer_block.deleted = 0
             GROUP BY dbsbuffer_block.blockname,
                      dbsbuffer_location.pnn,
                      dbsbuffer_dataset.path,
                      dbsbuffer_dataset_subscription.site
             HAVING COUNT(*) = SUM(dbsbuffer_workflow.completed)
             AND COUNT(wmbs_workflow.name) = 0
             """

    def execute(self, conn=None, transaction=False):

        results = self.dbi.processData(self.sql, conn=conn,
                                       transaction=transaction)[0].fetchall()

        blockDict = {}
        for result in results:

            blockName = result[0]
            location = result[1]
            dataset = result[2]
            site = result[3]

            if blockName not in blockDict:
                blockDict[blockName] = {'location': location,
                                        'dataset': dataset,
                                        'sites': set()}

            blockDict[blockName]['sites'].add(site)

        return blockDict
