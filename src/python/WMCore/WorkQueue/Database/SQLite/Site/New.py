"""
_New_

SQLite implementation of site.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.1 2009/08/18 23:18:16 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Site.New import New \
     as NewMySQL

class New(NewMySQL):
    sql = """INSERT OR IGNORE INTO wq_site (name) VALUES (:name)"""
