#!/usr/bin/env python
"""
_New_

Oracle implementation of JobGroup.New
"""

__all__ = []



import time

from WMCore.WMBS.MySQL.JobGroup.New import New as NewJobGroupMySQL

class New(NewJobGroupMySQL):
    sql = """INSERT INTO wmbs_jobgroup (id, subscription, guid, output,
             last_update) VALUES (wmbs_jobgroup_SEQ.nextval, :subscription,
             :guid, :output, %d)""" % time.time()
