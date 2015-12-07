# -*- coding: utf-8 -*-
"""
_SetState_

MySQL implementation of Locations.SetState

Created on Mon Jun 18 13:16:00 2012

@author: dballest
"""


from WMCore.Database.DBFormatter import DBFormatter

class SetState(DBFormatter):

    sql = """UPDATE wmbs_location
             SET state = (SELECT id FROM wmbs_location_state WHERE name = :state)
             WHERE site_name = :location
             """

    def execute(self, siteName, state, conn = None, transaction = False):
        binds = {"location": siteName, "state": state}
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
