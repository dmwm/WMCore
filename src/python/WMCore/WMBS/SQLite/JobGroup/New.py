#!/usr/bin/env python
"""
_New_

SQLite implementation of JobGroup.New
"""

__all__ = []



from WMCore.WMBS.MySQL.JobGroup.New import New as NewMySQL

class New(NewMySQL):
    sql = """INSERT INTO wmbs_jobgroup (subscription, uid, output,
             last_update) VALUES (:subscription, :guid, :output,
             strftime('%s', 'now'))"""
