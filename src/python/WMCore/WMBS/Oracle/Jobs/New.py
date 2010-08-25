#!/usr/bin/env python
"""
_New_

Oracle implementation of Jobs.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.5 2009/09/10 16:47:28 mnorman Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.WMBS.MySQL.Jobs.New import New as NewJobMySQL

class New(NewJobMySQL):
    sql = """INSERT INTO wmbs_job (id, jobgroup, name, state, state_time, 
                                   couch_record, cache_dir, location) VALUES 
              (wmbs_job_SEQ.nextval, :jobgroup, :name,
               (SELECT id FROM wmbs_job_state WHERE name = 'new'),
               :state_time, :couch_record, :cache_dir,
               (SELECT id FROM wmbs_location WHERE site_name = :location))"""
