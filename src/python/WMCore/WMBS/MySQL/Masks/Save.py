#!/usr/bin/env python
"""
_Save_

MySQL implementation of Masks.Save
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter



class Save(DBFormatter):
    sql = """INSERT IGNORE INTO wmbs_job_mask (job, firstevent, lastevent, firstrun,
               lastrun, firstlumi, lastlumi, inclusivemask)
             VALUES (:jobid, :firstevent, :lastevent, :firstrun, :lastrun, :firstlumi,
               :lastlumi, :inclusivemask)
    """

    def execute(self, jobid, mask, conn = None, transaction = False):
        if isinstance(mask, list):
            # Bulk commit
            # Hope you didn't send us a list of empty masks
            binds = []
            for m in mask:
                binds.append({"jobid": m['jobID'], 'firstevent': m['FirstEvent'], 'lastevent': m['LastEvent'],
                              'firstrun': m['FirstRun'], 'lastrun': m['LastRun'], 'firstlumi': m['FirstLumi'],
                              'lastlumi': m['LastLumi'], 'inclusivemask': m['inclusivemask']})
        else:
            # Simple one-part mask
            binds = {"jobid": jobid, 'firstevent': mask['FirstEvent'], 'lastevent': mask['LastEvent'],
                     'firstrun': mask['FirstRun'], 'lastrun': mask['LastRun'], 'firstlumi': mask['FirstLumi'],
                     'lastlumi': mask['LastLumi'], 'inclusivemask': mask['inclusivemask']}

            fail = True
            for key in binds:
                if key != 'jobid' and key != 'inclusivemask' and binds[key] != None:
                    # At least one of the keys contains something
                    fail = False
            if fail:
                return

        # Actually run the code
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)

        return
