#!/usr/bin/env python
"""
_GetRequestByInput_

Get request ID for an input datasets

"""





from WMCore.Database.DBFormatter import DBFormatter

class GetRequestByInput(DBFormatter):
    """
    _GetRequestByInput_

    Get input dataset associated to a request id

    """
    def execute(self, datasetName,
                conn = None, trans = False):
        """
        _execute_

        """
        self.sql = """
           select request_id from reqmgr_input_dataset
             where dataset_name = :datasetname"""
        binds = {"datasetname": datasetName}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
        return self.format(result)
