#!/usr/bin/env python
"""
_InsertThreshold_

This module inserts thresholds for the given sites for MySQL.  If the thresholds
have already been inserted this will modify them.
"""




from WMCore.Database.DBFormatter import DBFormatter

class InsertThreshold(DBFormatter):
    """
    _InsertThreshold_

    This module inserts thresholds for the given sites for MySQL.  If the thresholds
    have already been inserted this will modify them.    
    """
    selSQL = """SELECT * FROM rc_threshold WHERE
                  site_id = (SELECT id FROM wmbs_location WHERE site_name = :sitename) AND
                  sub_type_id = (SELECT id FROM wmbs_sub_types WHERE name  = :tasktype)"""

    updSQL = """UPDATE rc_threshold SET max_slots = :maxslots
                WHERE site_id = (SELECT id FROM wmbs_location WHERE site_name = :sitename) AND
                      sub_type_id = (SELECT id FROM wmbs_sub_types WHERE name  = :tasktype)"""
    
    addSQL = """INSERT INTO rc_threshold (site_id, sub_type_id, max_slots) VALUES (
                 (SELECT id FROM wmbs_location WHERE site_name = :sitename),
                 (SELECT id FROM wmbs_sub_types WHERE name  = :tasktype),
                 :maxslots)"""

    def execute(self, siteName, taskType, maxSlots, conn = None,
                transaction = False):
        binds = {"sitename": siteName, "tasktype": taskType,
                 "maxslots": maxSlots}
        result = self.dbi.processData(self.selSQL, {"sitename": siteName, "tasktype": taskType},
                                      conn = conn, transaction = transaction)
        result = self.format(result)

        if len(result) == 0:
            self.dbi.processData(self.addSQL, binds, conn = conn, transaction = transaction)
        else:
            self.dbi.processData(self.updSQL, binds, conn = conn, transaction = transaction)            

        return
