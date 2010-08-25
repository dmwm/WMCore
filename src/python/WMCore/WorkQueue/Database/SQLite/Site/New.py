"""
_New_

SQLite implementation of site.New
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Site.New import New \
     as NewMySQL

class New(NewMySQL):
    sql = """INSERT OR IGNORE INTO wq_site (name) VALUES (:name)"""
