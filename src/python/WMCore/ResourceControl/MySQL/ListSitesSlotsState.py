#!/usr/bin/env python
"""
_ListSitesSlotsState_

Query the Resource Control database to get all the current sites,
running slots, pending slots and state.
"""
from WMCore.Database.DBFormatter import DBFormatter

class ListSitesSlotsState(DBFormatter):
    sql = """SELECT site_name, running_slots, pending_slots,
                 wlst.name AS state FROM wmbs_location
                 INNER JOIN wmbs_location_state wlst ON wlst.id = wmbs_location.state
           """

    def execute(self, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, conn = conn, transaction = transaction)
        results = self.formatDict(results)

        dictResults = {}
        for elem in results:
            dictResults[elem['site_name']] = {'running_slots': elem['running_slots'],
                                              'pending_slots': elem['pending_slots'],
                                              'state': elem['state']}
        return dictResults
