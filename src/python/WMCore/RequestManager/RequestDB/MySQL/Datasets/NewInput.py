#!/usr/bin/env python
"""
_Datasets.NewInput_

Add a new Input Dataset details to a request


"""



from WMCore.Database.DBFormatter import DBFormatter

class NewInput(DBFormatter):
    """
    _NewInput_

    New input dataset associated to a request

    """
    def execute(self, requestId, datasetName, datasetType = "source",
                conn = None, trans = False):
        """
        _execute_

        Associate an input dataset name to the request id provided

        The dataset type defaults tp source, but could be set to
        secondary or pileup

        """
        self.sql = "INSERT INTO reqmgr_input_dataset ("
        self.sql += "request_id, dataset_name, dataset_type) VALUES "
        self.sql += "(%s, \'%s\', \'%s\') " % (
            requestId, datasetName, datasetType)

        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        return

