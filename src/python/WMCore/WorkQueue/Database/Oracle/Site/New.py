"""
_New_

SQLite implementation of Site.New
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Site.New import New \
     as NewMySQL

class New(NewMySQL):
    sql = """INSERT INTO wq_site (name)
                SELECT :name FROM DUAL
                  WHERE NOT EXISTS
                    (SELECT name FROM wq_site WHERE name = :name)"""