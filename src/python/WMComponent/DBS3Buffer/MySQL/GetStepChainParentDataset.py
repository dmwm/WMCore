#!/usr/bin/env python
"""
_GetStepChainParentDataset_

MySQL implementation of DBSBuffer.GetStepChainParentDataset
"""

from WMCore.Database.DBFormatter import DBFormatter

class GetStepChainParentDataset(DBFormatter):
    """
    _GetStepChainParentDataset_

    If given dataset name, get blockname
    """
    sql = """ SELECT DISTINCT parent FROM dbsbuffer_dataset
                WHERE step_chain = 1 and dbsbuffer_dataset.Path = :dataset
          """

    def execute(self, dataset, conn = None, transaction=False):
        """
        _execute_

        Either the dataset's ID or path must be specified.
        """
        binds = {'dataset': dataset}
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                          transaction = transaction)

        return self.formatOne(result)