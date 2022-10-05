"""
_CountUndeletedBlocksByWorkflow_

MySQL implementation of Workflows.CountUndeletedBlocksByWorkflow

Retrieves a list of workflows and the relative undeleted blocks counters,
"""


from WMCore.Database.DBFormatter import DBFormatter


class CountUndeletedBlocksByWorkflow(DBFormatter):
    """
    Retrieves a list of all workflows and the relative undeleted blocks counters

    The structure returned:
    [{'count': 6,
      'deleted': 0,
      'name': 'PromptReco_Run351572_HcalNZS_Tier0_REPLAY_2022_ID220531142559_v425_220531_1430'},
     {'count': 8,
      'deleted': 0,
      'name': 'PromptReco_Run351572_NoBPTX_Tier0_REPLAY_2022_ID220531142559_v425_220531_1430'},
      ...]
    """
    sql = """
    SELECT
        dbsbuffer_workflow.name,
        COUNT(DISTINCT dbsbuffer_block.blockname) as count
    FROM dbsbuffer_block
    INNER JOIN dbsbuffer_file ON
        dbsbuffer_file.block_id = dbsbuffer_block.id
    INNER JOIN dbsbuffer_workflow ON
        dbsbuffer_workflow.id = dbsbuffer_file.workflow
    WHERE dbsbuffer_block.deleted=0
    GROUP BY
        dbsbuffer_workflow.name
    """

    def execute(self, conn=None, transaction=False, returnCursor=False):
        """
        Executing the current sql query.
        :param conn:        A current database connection to be used if existing
        :param transaction: A current database transaction to be used if existing
        :return:            A list of dictionaries one record for each database line returned
        """
        dictResults = DBFormatter.formatDict(self, self.dbi.processData(self.sql, conn=conn,
                                                                        transaction=transaction))
        return dictResults
