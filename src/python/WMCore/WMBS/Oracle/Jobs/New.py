#!/usr/bin/env python
"""
_New_

Oracle implementation of Jobs.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.6 2009/12/22 16:09:39 mnorman Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.WMBS.MySQL.Jobs.New import New as NewJobMySQL

class New(NewJobMySQL):
    sql = """INSERT INTO wmbs_job (id, jobgroup, name, state, state_time, 
                                   couch_record, cache_dir, location, outcome, fwjr_path) VALUES 
              (wmbs_job_SEQ.nextval, :jobgroup, :name,
               (SELECT id FROM wmbs_job_state WHERE name = 'new'),
               :state_time, :couch_record, :cache_dir,
               (SELECT id FROM wmbs_location WHERE site_name = :location),
               :outcome, :fwjr_path)"""
