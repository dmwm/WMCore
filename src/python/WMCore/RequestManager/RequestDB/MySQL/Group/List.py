#!/usr/bin/env python
"""
_List_

Get a list of physics group names within the request system

"""







from WMCore.Database.DBFormatter import DBFormatter


class List(DBFormatter):
    """
    _List_

    Get a list of group names

    """
    def execute(self, conn = None, trans = False):
        """
        _execute_

        Retrieve a list of group names from the database

        """
        self.sql = "select group_name from reqmgr_group"
        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        output = [ x[0] for x in self.format(result)]
        return output
