#!/usr/bin/env python
"""
_GetOutput_

Get output datasets for a request ID

"""


__revision__ = "$Id: GetOutput.py,v 1.1 2010/07/01 19:04:43 rpw Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetOutput(DBFormatter):
    """
    _GetOutput_

    Get output dataset associated to a request id

    """
    def execute(self, requestId,
                conn = None, trans = False):
        """
        _execute_

        """

        self.sql = """
           select dataset_name from reqmgr_output_dataset
             where request_id = %s""" % requestId
        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        return self.format(result)



