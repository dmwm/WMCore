"""
_New_

SQLite implementation of WorkQueueElement.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.2 2009/08/18 23:18:12 swakef Exp $"
__version__ = "$Revision: 1.2 $"


from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.New import New \
     as NewMySQL
     
class New(NewMySQL):
    sql = NewMySQL.sql
    
