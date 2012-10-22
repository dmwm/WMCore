#!/usr/bin/env python
"""
_GetInput_

Get input datasets for a request ID

"""





from WMCore.Database.DBFormatter import DBFormatter

class GetInput(DBFormatter):
    """
    _GetInput_

    Get input dataset associated to a request id

    """
    def execute(self, requestId,
                conn = None, trans = False):
        """
        _execute_

        """

        self.sql = """
           select dataset_name, dataset_type from reqmgr_input_dataset
             where request_id = :request_id"""
        binds = {"request_id": requestId}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
        return dict(self.format(result))
