#!/usr/bin/env python
"""
_New_

MySQL implementation of Jobs.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.10 2009/05/12 16:17:54 sfoulkes Exp $"
__version__ = "$Revision: 1.10 $"

import time

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT INTO wmbs_job (jobgroup, name, state, state_time, 
                                   couch_record, location) VALUES 
              (:jobgroup, :name,
               (SELECT id FROM wmbs_job_state WHERE name = 'new'),
               :state_time, :couch_record, 
               (SELECT id FROM wmbs_location WHERE site_name = :location))"""

    def execute(self, jobgroup, name, couch_record = None, location = None, 
                conn = None, transaction = False):
        binds = {"jobgroup": jobgroup, "name": name, 
                 "couch_record": couch_record, "state_time": int(time.time()),
                 "location": location}

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)            
        return
