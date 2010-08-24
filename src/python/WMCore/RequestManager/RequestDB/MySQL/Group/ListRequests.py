#!/usr/bin/env python
"""
_ListRequests_

List requests for a group

"""




from WMCore.Database.DBFormatter import DBFormatter


class ListRequests(DBFormatter):
    """
    _ListRequests_

    Get requests for a given group Id

    """

    def execute(self, groupId, conn = None, trans = False):
        """
        _execute_

        Get the list of requests for the requestor specified

        """
        self.sql = """
        select request_name, request_id from reqmgr_request
           JOIN reqmgr_group_association ON
   reqmgr_request.requestor_group_id = reqmgr_group_association.association_id
           where reqmgr_group_association.group_id = %s
           """ % groupId

        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        return dict(self.format(result))



