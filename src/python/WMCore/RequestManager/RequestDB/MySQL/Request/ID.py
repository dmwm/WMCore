#!/usr/bin/env python
"""
_Request.ID_

API for getting a request id from its name

"""
__revision__ = "$Id: ID.py,v 1.1 2010/07/01 19:12:39 rpw Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.Database.DBFormatter import DBFormatter

class ID(DBFormatter):
    """
    _ID_

    retrieve the details of a request given a request id

    """
    def execute(self, requestName, conn = None, trans = False):
        """
        _execute_

        map the request name provided to an ID

        """
        self.sql = "SELECT request_id from reqmgr_request WHERE "
        self.sql += "request_name=\'%s\'" % requestName
        result = self.executeOne(conn = conn, transaction = trans)
        if result == []:
            return None
        return result[0]

