"""
_GetStateID_

MySQL implementation of Jobs.GetStateID

Created on Sep 13, 2012

@author: dballest
"""
from WMCore.Database.DBFormatter import DBFormatter

class GetStateID(DBFormatter):
    """
    DAO to retrieve the id of a given job state name
    """

    sql = """SELECT id
             FROM wmbs_job_state
             WHERE name = :stateName
          """

    def execute(self, state = 'created', conn = None, transaction = False):

        result = self.dbi.processData(self.sql, {'stateName' : state},
                             conn = conn, transaction = transaction)

        stateId = self.formatOne(result)

        return stateId[0]
