#!/usr/bin/env python
"""
_ListAlgoDatasetAssoc_

MySQL implementation of DBSBuffer.ListAlgoDatasetAssoc
"""




from WMCore.Database.DBFormatter import DBFormatter

class ListAlgoDatasetAssoc(DBFormatter):
    """
    _ListDataset_

    Retrieve information about a dataset/algorthim association in the DBSBuffer.
    This is mostly used by the unit tests.
    """
    sql = """SELECT id, algo_id, dataset_id, in_dbs FROM dbsbuffer_algo_dataset_assoc
               WHERE id = :id"""

    def execute(self, assocID = None, conn = None, transaction = False):
        """
        _execute_

        Retrieve information about a dataset/algorithm association in DBSBuffer.
        """
        binds = {"id": assocID}
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                          transaction = transaction)

        return self.formatDict(result)
