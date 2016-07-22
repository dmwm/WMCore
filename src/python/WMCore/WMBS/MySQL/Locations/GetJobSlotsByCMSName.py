#!/usr/bin/env python
"""
_GetJobSlotsByCMSName_
"""

from WMCore.Database.DBFormatter import DBFormatter


class GetJobSlotsByCMSName(DBFormatter):
    """
    Get site running and pending thresholds grouped by CMS name
    """

    sql = """
        SELECT cms_name, wmbs_location_state.name as state, pending_slots, running_slots
            FROM wmbs_location
            INNER JOIN wmbs_location_state ON wmbs_location.state = wmbs_location_state.id
        """

    def formatDict(self, results):
        """
        _formatDict_

        Format the results in a dict keyed by the cms_name
        """
        formattedResults = DBFormatter.formatDict(self, results)

        dictResult = {}
        for item in formattedResults:
            site = item.pop('cms_name')
            dictResult[site] = item

        return dictResult

    def execute(self, conn=None, transaction=False, returnCursor=False):
        results = self.dbi.processData(self.sql, conn=conn, transaction=transaction)
        return self.formatDict(results)
