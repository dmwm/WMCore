#!/usr/bin/env python
"""
_Map_

Get map of request type to request type id

"""





from WMCore.Database.DBFormatter import DBFormatter

class Map(DBFormatter):
    """
    _Map_

    Get a map of request type to id from the DB

    """
    def execute(self, conn = None, trans = False):
        """
        _execute_

        Get mapping of type values to ids

        """
        self.sql = "SELECT type_name, type_id FROM reqmgr_request_type"

        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)

        return dict(result[0].fetchall())
