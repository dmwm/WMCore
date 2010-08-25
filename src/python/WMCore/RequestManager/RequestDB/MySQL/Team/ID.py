#!/usr/bin/env python
"""
_Team.ID_

Get the id for an operations team

"""



from WMCore.Database.DBFormatter import DBFormatter

class ID(DBFormatter):
    """
    _ID_

    retrieve the team_id from the team_name provided, return None
    if the team doesnt exist


    """
    def execute(self, teamName, conn = None, trans = False):
        """
        _execute_

        get the team id for the teamName provided else None

        """
        self.sql = """
        select team_id from reqmgr_teams where team_name = '%s'
        """ % teamName

        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        output = self.format(result)
        if len(output) == 0:
            return None
        return output[0][0]


