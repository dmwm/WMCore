#!/usr/bin/env python
"""
_Request.SetStatus_

Change a requests status

"""



from WMCore.Database.DBFormatter import DBFormatter

class SetStatus(DBFormatter):
    """
    _SetStatus_

    Update a requests status value to a new value


    """
    def execute(self, requestId, statusId, conn = None, trans = False):
        """
        _execute_

        Update reqmgr_request status field to the new status value

        """
        self.sql = """
        UPDATE reqmgr_request SET request_status=%s
          WHERE request_id = %s
        """ % (statusId, requestId)

        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        return




