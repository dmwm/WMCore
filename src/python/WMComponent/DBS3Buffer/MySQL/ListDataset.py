#!/usr/bin/env python
"""
_ListDataset_

MySQL implementation of DBSBuffer.ListDataset
"""




from WMCore.Database.DBFormatter import DBFormatter

class ListDataset(DBFormatter):
    """
    _ListDataset_

    Retrieve information about a dataset in the DBSBuffer.  This is mostly used
    by the unit tests.
    """
    sql = """SELECT id, path, acquisition_era, processing_ver,
                    valid_status, global_tag, parent, prep_id FROM dbsbuffer_dataset
               WHERE path = :path"""

    sqlID = """SELECT id, path, acquisition_era, processing_ver,
                    valid_status, global_tag, parent, prep_id FROM dbsbuffer_dataset
                 WHERE id = :id"""

    def execute(self, datasetID = None, datasetPath = None, conn = None,
                transaction = False):
        """
        _execute_

        Either the dataset's ID or path must be specified.
        """
        if datasetID == None:
            binds = {"path": datasetPath}
            result = self.dbi.processData(self.sql, binds, conn = conn,
                                          transaction = transaction)
        else:
            binds = {"id": datasetID}
            result = self.dbi.processData(self.sqlID, binds, conn = conn,
                                          transaction = transaction)

        return self.formatDict(result)
