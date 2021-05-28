#!/usr/bin/env python
"""
_Save_

Oracle implementation of Masks.Save
"""

__all__ = []

from WMCore.WMBS.MySQL.Masks.Save import Save as SaveMasksMySQL


class Save(SaveMasksMySQL):
    sql = """INSERT INTO wmbs_job_mask (job, firstevent, lastevent, firstrun, lastrun, firstlumi, lastlumi, inclusivemask)
             SELECT :jobid, :firstevent, :lastevent, :firstrun, :lastrun, :firstlumi, :lastlumi, :inclusivemask FROM dual
             WHERE NOT EXISTS (SELECT * FROM wmbs_job_mask wjm2 WHERE
                               job = :jobid
                               AND wjm2.firstevent = :firstevent
                               AND wjm2.lastevent = :lastevent
                               AND wjm2.firstlumi = :firstlumi
                               AND wjm2.lastlumi = :lastlumi
                               AND wjm2.firstrun = :firstrun
                               AND wjm2.lastrun = :lastrun
                               AND wjm2.inclusivemask = :inclusivemask)"""

    def execute(self, jobid, mask, conn=None, transaction=False):
        if isinstance(mask, list):
            # Bulk commit
            # Hope you didn't send us a list of empty masks
            binds = []
            for m in mask:
                inclusiveMask = 'T'
                if m['inclusivemask'] is False:
                    inclusiveMask = 'F'
                binds.append({"jobid": m['jobID'], 'firstevent': m['FirstEvent'], 'lastevent': m['LastEvent'],
                              'firstrun': m['FirstRun'], 'lastrun': m['LastRun'], 'firstlumi': m['FirstLumi'],
                              'lastlumi': m['LastLumi'], 'inclusivemask': inclusiveMask})
        else:
            # Simple one-part mask
            inclusiveMask = 'T'
            if mask['inclusivemask'] is False:
                inclusiveMask = 'F'
            binds = {"jobid": jobid, 'firstevent': mask['FirstEvent'], 'lastevent': mask['LastEvent'],
                     'firstrun': mask['FirstRun'], 'lastrun': mask['LastRun'], 'firstlumi': mask['FirstLumi'],
                     'lastlumi': mask['LastLumi'], 'inclusivemask': inclusiveMask}

            fail = True
            for key in binds:
                if key != 'jobid' and key != 'inclusivemask' and binds[key] != None:
                    # At least one of the keys contains something
                    fail = False
            if fail:
                return

        # Actually run the code
        self.dbi.processData(self.sql, binds, conn=conn, transaction=transaction)

        return
