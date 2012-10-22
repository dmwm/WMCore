#!/usr/bin/env python
"""
_Datasets.NewOutput_

Add a new Output Dataset details to a request


"""



from WMCore.Database.DBFormatter import DBFormatter

class NewOutput(DBFormatter):
    """
    _NewOutput_

    New output dataset associated to a request

    """
    def execute(self, requestId, datasetName,
                conn = None, trans = False):
        """
        _execute_

        Associate an output dataset name to the request id provided

        """
        self.sql = "INSERT INTO reqmgr_output_dataset ("
        self.sql += "request_id, dataset_name) VALUES "
        self.sql += "(:request_id, :dataset_name) "
        binds = {"request_id": requestId, "dataset_name": datasetName}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
        return
