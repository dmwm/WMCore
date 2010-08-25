#!/usr/bin/env python
"""
_Datasets.NewOutput_

Add a new Output Dataset details to a request


"""
__revision__ = "$Id: NewOutput.py,v 1.1 2010/07/01 19:04:43 rpw Exp $"
__version__ = "$Revision: 1.1 $"

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
        self.sql += "(%s, \'%s\') " % (
            requestId, datasetName)

        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        return

