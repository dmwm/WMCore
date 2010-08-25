#!/usr/bin/env python
"""
_Map_

Get map of status to status id

"""





from WMCore.Database.DBFormatter import DBFormatter

class Map(DBFormatter):
    """
    _Map_

    Get a map of status name to id from the DB

    """
    def execute(self, conn = None, trans = False):
        """
        _execute_

        Get mapping of status values to ids

        """
        self.sql = "SELECT status_name, status_id FROM reqmgr_request_status"

        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)

        return dict(result[0].fetchall())
