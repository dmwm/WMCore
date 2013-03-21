"""
_Assignment.New_

Create a new assignment between a team and a request

"""

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    """
    _New_

    Create a new assignment from a request to a production team

    """
    def execute(self, requestId, teamId,conn = None, trans = False):
        """
        _execute_

        Make a new association between a request and a prod team

        """
        self.sql = "INSERT INTO reqmgr_assignment "
        self.sql += "(request_id, team_id) "
        self.sql += "VALUES (:requestId, :teamId)"

        binds = {"requestId":requestId, "teamId":teamId}

        result = self.dbi.processData(self.sql, binds, conn = conn, 
                 transaction = trans)
