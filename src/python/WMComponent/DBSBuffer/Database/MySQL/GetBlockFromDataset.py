#!/usr/bin/env python
"""
_ListDataset_

MySQL implementation of DBSBuffer.GetBlockFromDataset
"""




from WMCore.Database.DBFormatter import DBFormatter

class GetBlockFromDataset(DBFormatter):
    """
    _GetBlockFromDataset_

    If given dataset name, get blockname
    """
    sql = """SELECT DISTINCT blockname FROM dbsbuffer_block
               INNER JOIN dbsbuffer_file ON dbsbuffer_block.id = dbsbuffer_file.block_id
               INNER JOIN dbsbuffer_algo_dataset_assoc ON dbsbuffer_file.dataset_algo = dbsbuffer_algo_dataset_assoc.id
               INNER JOIN dbsbuffer_dataset ON dbsbuffer_algo_dataset_assoc.dataset_id = dbsbuffer_dataset.id
               WHERE dbsbuffer_dataset.Path = :dataset
    """

    def execute(self, dataset = None, conn = None,
                transaction = False):
        """
        _execute_

        Either the dataset's ID or path must be specified.
        """

        binds = {'dataset': dataset}

        result = self.dbi.processData(self.sql, binds, conn = conn,
                                          transaction = transaction)

        return self.formatDict(result)
