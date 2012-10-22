#!/usr/bin/env python
"""
_GetRequestByOutput_

Get request ID for an output datasets

"""





from WMCore.Database.DBFormatter import DBFormatter

class GetRequestByOutput(DBFormatter):
    """
    _GetRequestByOutput_

    Get output dataset associated to a request id

    """
    def execute(self, datasetName,
                conn = None, trans = False):
        """
        _execute_

        """

        self.sql = """
           select request_id from reqmgr_output_dataset
             where dataset_name = :datasetname"""
        binds = {"datasetname": datasetName}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
        return self.format(result)
