"""
_GetCompletedBlocks_

MySQL implementation of RucioInjector.GetClompletedBlocks

Retrieve a list of blocks that are complete,
including their location and the sites they
are subscribed too

"""


from WMCore.Database.DBFormatter import DBFormatter


class GetCompletedBlocks(DBFormatter):
    """
    Retrieves a list of blocks that are closed but NOT sure yet if they are deleteable:
      - The workflows for all files in the block need to be completed (This relates only to the
        workflows directly producing the files and does not track child workflows completion)
      - The subscription made for dataset is copy+delete
      - A subscription has been made at the Data Management system
      - The blocks hasn't been deleted yet
    """
    sql = """SELECT dbsbuffer_block.blockname,
                    dbsbuffer_location.pnn,
                    dbsbuffer_dataset.path,
                    dbsbuffer_dataset_subscription.site,
                    dbsbuffer_workflow.name,
                    dbsbuffer_block.create_time
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
             WHERE dbsbuffer_dataset_subscription.delete_blocks = 1
               AND dbsbuffer_dataset_subscription.subscribed = 1
               AND dbsbuffer_block.status = 'Closed'
               AND dbsbuffer_block.deleted = 0
             GROUP BY dbsbuffer_block.blockname,
                      dbsbuffer_location.pnn,
                      dbsbuffer_dataset.path,
                      dbsbuffer_dataset_subscription.site,
                      dbsbuffer_workflow.name,
                      dbsbuffer_block.create_time
             """

    def format(self, result):
        """
        _format_

        Format the query results into the proper dictionary expected at the upper layer Python code.
        The input should be a list of database objects each representing a line returned from the database
        with key names matching the column names from the sql query
        The result should be a list of dictionaries one record per block returned from the database
        with key names mapped to the python code expected structures. All workflows and sites are aggregated 
        into the same block record.

        e.g.
        { '/Tau/Run2022C-PromptReco-v1/MINIAOD#2dd5a82b-873a-4403-8da1-6b943dac7081': {'blockCreateTime': 1659675842,
                                                                              'blockName': '/Tau/Run2022C-PromptReco-v1/MINIAOD#2dd5a82b-873a-4403-8da1-6b943dac7081',
                                                                              'dataset': '/Tau/Run2022C-PromptReco-v1/MINIAOD',
                                                                              'location': 'T0_CH_CERN_Disk',
                                                                              'sites': {'T1_ES_PIC_Disk',
                                                                                        'T1_ES_PIC_MSS'},
                                                                              'workflowNames': {'PromptReco_Run356614_Tau'}},
          '/Tau/Run2022C-PromptReco-v1/MINIAOD#f6bf5cc7-cab2-4572-8f30-574296bb109d': {'blockCreateTime': 1659723755,
                                                                              'blockName': '/Tau/Run2022C-PromptReco-v1/MINIAOD#f6bf5cc7-cab2-4572-8f30-574296bb109d',
                                                                              'dataset': '/Tau/Run2022C-PromptReco-v1/MINIAOD',
                                                                              'location': 'T0_CH_CERN_Disk',
                                                                              'sites': {'T1_ES_PIC_Disk',
                                                                                        'T1_ES_PIC_MSS'},
                                                                              'workflowNames': {'PromptReco_Run356615_Tau',
                                                                                                'PromptReco_Run356619_Tau'}}
        }


        NOTE:
         * location: Means where the output block has been created
         * site(s):  Means where the dataset gets a container-level rule

        :param result: The result as returned by the mysql query execution.
        :return:       Dictionary of dictionaries, each one describing a block.
        """

        # NOTE: We need to rename all the keys to follow the cammelCase standard. And also to comply
        #       with the key names as expected from the rest of the already existing python code
        keyMap = {'blockname': 'blockName',
                  'name': 'workflowNames',
                  'pnn': 'location',
                  'site': 'sites',
                  'path': 'dataset',
                  'create_time': 'blockCreateTime'}

        listResults = DBFormatter.formatDict(self, result)
        dictResults = {}
        for record in listResults:
            # Populates results dict and adds all workflows and sites of the same block to a single record
            blockName = record['blockname']
            if blockName in dictResults:
                dictResults[blockName]['workflowNames'].add(record['name'])
                dictResults[blockName]['sites'].add(record['site'])
            else:
                for dbKey, pyKey in keyMap.items():
                    if dbKey == 'site' or dbKey == 'name':
                        data = record.pop(dbKey)
                        record[pyKey] = set()
                        record[pyKey].add(data)
                    else:
                        record[pyKey] = record.pop(dbKey)
                dictResults[blockName] = record

        return dictResults

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
