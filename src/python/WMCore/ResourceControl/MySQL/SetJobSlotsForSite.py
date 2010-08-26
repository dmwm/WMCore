#!/usr/bin/env python
"""
_SetJobSlotsForSite_

Set the number of job slots available at a given site.
"""

__revision__ = "$Id: SetJobSlotsForSite.py,v 1.1 2010/07/15 16:57:05 sfoulkes Exp $"
__version__  = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class SetJobSlotsForSite(DBFormatter):
    sql = "UPDATE wmbs_location SET job_slots = :slots WHERE site_name = :site"

    def execute(self, siteName, jobSlots, conn = None, transaction = False):
        self.dbi.processData(self.sql, {"site": siteName, "slots": jobSlots},
                             conn = conn, transaction = transaction)
        return
