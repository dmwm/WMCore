#!/usr/bin/env python
"""
_Save_

MySQL implementation of Jobs.Save
"""

__all__ = []
__revision__ = "$Id: Save.py,v 1.6 2009/05/11 14:47:49 sfoulkes Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.Database.DBFormatter import DBFormatter

class Save(DBFormatter):
    sql = """UPDATE wmbs_job SET jobgroup = :jobgroup, name = :name, 
               couch_record = :couch_record, outcome = :outcome, location = 
                 (SELECT id FROM wmbs_location WHERE site_name = :location)
             WHERE id = :jobid"""
    
    def execute(self, jobid, jobgroup, name, couch_record, location, outcome, 
                conn = None, transaction = False):
        if outcome == 'success':
            boolOutcome = 0
        else:
            boolOutcome = 1
        
        binds = {"jobid": jobid, "jobgroup": jobgroup, "name": name, 
                 "couch_record": couch_record, "location": location, 
                 "outcome": boolOutcome}

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
