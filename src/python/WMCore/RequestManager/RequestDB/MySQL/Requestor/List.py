#!/usr/bin/env python
"""
_ListRequests_

List requests for a user

"""

from WMCore.Database.DBFormatter import DBFormatter


class List(DBFormatter):
    def execute(self, conn = None, trans = False):
        """
        _execute_

        Retrieve a list of user names from the database

        """
        self.sql = "select requestor_hn_name from reqmgr_requestor"
        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        output = [ x[0] for x in self.format(result)]
        return output

