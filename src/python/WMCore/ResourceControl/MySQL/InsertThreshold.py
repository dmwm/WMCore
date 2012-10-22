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

    updSQL = """UPDATE rc_threshold SET max_slots = :maxslots, priority = :priority
                WHERE site_id = (SELECT id FROM wmbs_location WHERE site_name = :sitename) AND
                      sub_type_id = (SELECT id FROM wmbs_sub_types WHERE name  = :tasktype)"""

    addSQL = """INSERT INTO rc_threshold (site_id, sub_type_id, max_slots, priority) VALUES (
                 (SELECT id FROM wmbs_location WHERE site_name = :sitename),
                 (SELECT id FROM wmbs_sub_types WHERE name  = :tasktype),
                 :maxslots, :priority)"""

    def execute(self, siteName, taskType, maxSlots, priority = None, conn = None,
                transaction = False):
        binds = {"sitename": siteName, "tasktype": taskType,
                 "maxslots": maxSlots, "priority": priority}
        result = self.dbi.processData(self.selSQL, {"sitename": siteName, "tasktype": taskType},
                                      conn = conn, transaction = transaction)
        result = self.formatDict(result)

        if len(result) == 0:
            if priority == None:
                binds['priority'] = 1
            self.dbi.processData(self.addSQL, binds, conn = conn, transaction = transaction)
        else:
            if priority == None:
                binds['priority'] = result[0]['priority']
            self.dbi.processData(self.updSQL, binds, conn = conn, transaction = transaction)

        return
