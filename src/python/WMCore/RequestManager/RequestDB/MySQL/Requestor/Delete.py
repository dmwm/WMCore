#!/usr/bin/env python
"""
_Delete_

Delete a user based on username

"""
__revision__ = "$Id: Delete.py,v 1.1 2010/07/01 19:14:16 rpw Exp $"
__version__ = "$Revision: 1.1 $"



from WMCore.Database.DBFormatter import DBFormatter


class Delete(DBFormatter):
    """
    _Delete_

    Remove a requestor

    """

    def execute(self, username, conn = None, trans = False):
        """
        _execute_

        Delete the user by username

        """
        self.sql = "delete from reqmgr_requestor where requestor_hn_name="
        self.sql += "\'%s\'" % username
        result = self.dbi.processData(self.sql,
                         conn = conn, transaction = trans)
        return
