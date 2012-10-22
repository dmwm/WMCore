#!/usr/bin/env python
"""
_New_

Oracle implementation of Jobs.New
"""

__all__ = []



from WMCore.WMBS.MySQL.Jobs.New import New as NewJobMySQL

class New(NewJobMySQL):
    sql = """INSERT INTO wmbs_job (id, jobgroup, name, state, state_time,
                                   couch_record, cache_dir, location, outcome, fwjr_path) VALUES
              (wmbs_job_SEQ.nextval, :jobgroup, :name,
               (SELECT id FROM wmbs_job_state WHERE name = 'new'),
               :state_time, :couch_record, :cache_dir,
               (SELECT id FROM wmbs_location WHERE site_name = :location),
               :outcome, :fwjr_path)"""
