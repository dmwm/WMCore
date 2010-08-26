#!/usr/bin/env python
"""
_Software.Association_

Associate a software version to a request

"""

__revision__ = "$Id: Association.py,v 1.1 2010/07/01 19:15:51 rpw Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class Association(DBFormatter):
    """

    _Association_

    Associate a request id with a software id

    """
    def execute(self, requestId, softwareId, conn = None, trans = False):
        """
        _execute_

        Associate the software id with the request id provided

        """
        self.sql = "INSERT INTO reqmgr_software_dependency ("
        self.sql += " request_id, software_id) VALUES "
        self.sql += " ( %s, %s )" % (requestId, softwareId)

        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)

        return

