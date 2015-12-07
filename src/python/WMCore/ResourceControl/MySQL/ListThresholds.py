#!/usr/bin/env python
"""
_ListThresholds_

Query the database to determine the threshold for each site.
"""




from WMCore.Database.DBFormatter import DBFormatter

class ListThresholds(DBFormatter):
    sql = """SELECT wl.site_name AS site, wst.name AS type,
                    rc.max_slots, rc.pending_slots FROM rc_threshold rc
                INNER JOIN wmbs_location wl ON wl.id = rc.site_id
                INNER JOIN wmbs_sub_types wst ON wst.id = rc.sub_type_id
                ORDER By site
           """

    def execute(self, conn = None, transaction = False):
        results = self.dbi.processData(self.sql,
                                       conn = conn, transaction = transaction)
        return self.formatDict(results)
