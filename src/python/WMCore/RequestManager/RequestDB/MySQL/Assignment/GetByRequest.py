#!/usr/bin/env python
"""
_GetByRequest_

DB API to get Assignments based on request id

"""



from WMCore.Database.DBFormatter import DBFormatter

class GetByRequest(DBFormatter):
    """
    _GetByRequest_

    get the assignment table entries for the request ID provided

    """
    def execute(self, requestId, conn = None, trans = False):
        """
        _execute_

        Return details of assignments for request id provided

        """
        self.sql = """
        SELECT assign.team_id, assign.priority_modifier, team.team_name
          FROM reqmgr_assignment assign
            JOIN reqmgr_teams team ON assign.team_id = team.team_id
          WHERE assign.request_id = %s
          """ % requestId

        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)

        output = []
        [ output.append({
            "TeamName" : x[2],
            "TeamID"   : x[0],
            "TeamPriority" : x[1],
            }) for x in self.format(result)]
        return output
