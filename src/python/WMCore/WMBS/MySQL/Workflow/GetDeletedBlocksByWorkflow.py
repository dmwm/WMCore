"""
_GetDeletedBlocksByWorkflow_

MySQL implementation of Workflows.GetDeletedBlocksByWorkflow

Retrieves a list of workflows with lists of deleted and NOT deleted blocks per workflow,

NOTE: This DAO is not used in the production code but is to be used only for debugging purposes
"""


from WMCore.Database.DBFormatter import DBFormatter


class GetDeletedBlocksByWorkflow(DBFormatter):
    """
    Retrieves a list of all workflows and the relative deleted blocks lists
    """
    sql = """SELECT
                 dbsbuffer_block.blockname,
                 dbsbuffer_block.deleted,
                 wmbs_workflow.name
             FROM dbsbuffer_block
             INNER JOIN dbsbuffer_file ON
                 dbsbuffer_file.block_id = dbsbuffer_block.id
             INNER JOIN dbsbuffer_workflow ON
                 dbsbuffer_workflow.id = dbsbuffer_file.workflow
             INNER JOIN wmbs_workflow ON
                 wmbs_workflow.name = dbsbuffer_workflow.name
             GROUP BY
                 dbsbuffer_block.blockname,
                 dbsbuffer_block.deleted,
                 wmbs_workflow.name
          """

    def format(self, result):
        """
        _format_

        Format the query results into the proper dictionary expected at the upper layer Python code.
        The input should be a list of database objects, each one representing a single line returned
        from the database with key names matching the column names from the sql query.

        The intermediate (not aggregated) result representing the primary database output in python should be
        a list of dictionaries one record per line returned from the database with key names mapped to the
        python code variable naming conventions.

        e.g.
        [{'blockname': '/a/b/c#123-qwe',
          'deleted': 0,
          'name': 'WorkflowName'},
        {'blockname': '/a/b/c#456-rty',
          'deleted': 1,
          'name': 'WorkflowName'},
         {'blockname': '/a/b/d#123-asd',
          'deleted': 0,
          'name': 'WorkflowName'}
        ...
        ]

        NOTE:
         * The number of records per workflow and block returned (i.e. number of records per group in the GROUP BY statement)
           from the query is not related to either the number of blocks nor to the number of workflows, but rather to the
           combination of number of files in the block and some other factor which increases the granularity (it seems to be
           the number of records in dbsbuffer_workflow table per file aggregated by workflow), and NO `DISTINCT` requirement
           in the SELECT statement is needed because we already have them properly grouped.
         * Once deleted we should NOT expect duplicate records with two different values of the deleted
           flag to be returned for a single block but we should still create the list of deleted and
           NotDeleted blocks as sets and eventually create their proper intersection for double check.

        This list needs to be further aggregated by name to produce an aggregated structure per workflow like:

        [{'name': 'WorkflowName'
          'blocksNotDeleted': ['/a/b/c#123-qwe',
                               /a/b/c#456-rty']
          'blocksDeleted': ['/a/b/d#123-asd']
         },
        ...
        ]

        :param result: The result as returned by the mysql query execution.
        :return:       List of dictionaries
        """

        # First reformat the output in a list of dictionaries per DB record
        dictResults = DBFormatter.formatDict(self, result)

        # Now aggregate all blocks per workflow:
        results = {}
        for record in dictResults:
            wfName = record['name']
            results.setdefault(wfName, {'name': wfName, 'blocksDeleted': [], 'blocksNotDeleted': []})
            if record['deleted']:
                results[wfName]['blocksDeleted'].append(record['blockname'])
            else:
                results[wfName]['blocksNotDeleted'].append(record['blockname'])
        return results.values()

    def execute(self, conn=None, transaction=False, returnCursor=False):
        """
        Executing the current sql query.
        :param conn:        A current database connection to be used if existing
        :param transaction: A current database transaction to be used if existing
        :return:            A list of dictionaries one record for each database line returned
        """
        results = self.dbi.processData(self.sql, conn=conn,
                                       transaction=transaction)

        return self.format(results)
