#!/usr/bin/env python
"""
_Assignment.Delete_

Delete the assignment between a team and a request

"""


__revision__ = "$Id: Delete.py,v 1.1 2010/07/01 19:03:09 rpw Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class Delete(DBFormatter):
    """
    _Delete_

    Delete the assignment from a request to a production team

    """
    def execute(self, requestId,
                conn = None, trans = False):
        """
        _execute_

        Delete the association between a request and a prod team

        """
        self.sql = "DELETE FROM reqmgr_assignment WHERE request_id = " % requestId
        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        return

