#!/usr/bin/env python
"""
_New_

Oracle implementation of Jobs.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.4 2009/05/11 14:47:49 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMBS.MySQL.Jobs.New import New as NewJobMySQL

class New(NewJobMySQL):
    sql = """INSERT INTO wmbs_job (id, jobgroup, name, state, state_time, 
                                   couch_record, location) VALUES 
              (wmbs_job_SEQ.nextval, :jobgroup, :name,
               (SELECT id FROM wmbs_job_state WHERE name = 'new'),
               :state_time, :couch_record, 
               (SELECT id FROM wmbs_location WHERE site_name = :location))"""
