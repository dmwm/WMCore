#!/usr/bin/env python
"""
_New_

SQLite implementation of JobGroup.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.3 2009/01/11 17:40:46 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.JobGroup.New import New as NewMySQL

class New(NewMySQL):
    sql = """INSERT INTO wmbs_jobgroup (subscription, uid, output,
             last_update) VALUES (:subscription, :guid, :output,
             strftime('%s', 'now'))"""
