"""
_New_

SQLite implementation of Site.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.1 2009/08/27 21:04:30 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Site.New import New \
     as NewMySQL

class New(NewMySQL):
    sql = """INSERT INTO wq_site (name)
                SELECT :name FROM DUAL
                  WHERE NOT EXISTS
                    (SELECT name FROM wq_site WHERE name = :name)"""