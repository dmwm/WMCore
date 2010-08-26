#!/usr/bin/env python
"""
_Request.GetGroup_

API for getting the group that owns a request

"""
__revision__ = "$Id: GetGroup.py,v 1.1 2010/07/01 19:12:39 rpw Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetGroup(DBFormatter):
    """
    _GetGroup_

    retrieve the group of a request given a request id

    """
    def execute(self, associationId, conn = None, trans = False):
        """
        _execute_

        retrieve details of a request given the request id

        """
        self.sql = "select * from reqmgr_group JOIN reqmgr_group_association ON reqmgr_group_association.group_id = reqmgr_group.group_id WHERE reqmgr_group_association.association_id = %s"


        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
