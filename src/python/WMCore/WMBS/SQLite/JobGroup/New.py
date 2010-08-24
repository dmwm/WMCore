#!/usr/bin/env python
"""
_New_

SQLite implementation of JobGroup.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.2 2008/11/21 17:10:28 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.JobGroup.New import New as NewMySQL

class New(NewMySQL):
    sql = []
    sql.append("""insert into wmbs_jobgroup (subscription, uid, output, last_update) values (:subscription, :uid, :output, strftime('%s', 'now'))""")
    sql.append("""select id from wmbs_jobgroup where uid=:uid""")
