#!/usr/bin/env python
"""
_Save_

MySQL implementation of Jobs.Save
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class Save(DBFormatter):
    sql = """UPDATE wmbs_job SET jobgroup = :jobgroup, name = :name,
               couch_record = :couch_record, outcome = :outcome, cache_dir = :cache_dir,
               location =
                 (SELECT id FROM wmbs_location WHERE site_name = :location),
               fwjr_path = :fwjr, retry_count = :retry_count
             WHERE id = :jobid"""

    def execute(self, jobid, jobgroup, name, couch_record, location, outcome, cache_dir, fwjr,
                retry_count = 0, conn = None, transaction = False):
        if outcome == 'success':
            boolOutcome = 1
        else:
            boolOutcome = 0

        binds = {"jobid": jobid, "jobgroup": jobgroup, "name": name,
                 "couch_record": couch_record, "location": location,
                 "outcome": boolOutcome, "cache_dir": cache_dir, "fwjr": fwjr,
                 'retry_count': retry_count}

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
