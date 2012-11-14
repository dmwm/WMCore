#!/usr/bin/env python
"""
_UpdateThresholdsBulk_

This module updates thresholds for the given sites for MySQL.
"""




from WMCore.Database.DBFormatter import DBFormatter

class UpdateThresholdsInBulk(DBFormatter):
    """
    _UpdateThresholdsBulk_
    """
    sql = """UPDATE rc_threshold SET max_slots = :maxslots, pending_slots = :pendslots
                WHERE site_id = (SELECT id FROM wmbs_location WHERE site_name = :sitename) AND
                      sub_type_id = (SELECT id FROM wmbs_sub_types WHERE name  = :tasktype)"""


    def execute(self, data, conn = None, transaction = False):

        """
        data is the list of dictionary. each dictionary contains following items.
        [{"sitename": T2_XX_SiteA, "tasktype": Processing, "maxslots": 1000}, ...]
        """
        self.dbi.processData(self.sql, data,
                             conn = conn, transaction = transaction)
        return
