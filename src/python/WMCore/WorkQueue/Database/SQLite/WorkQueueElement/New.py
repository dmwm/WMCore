"""
_New_

SQLite implementation of WorkQueueElement.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.3 2009/09/03 15:44:16 swakef Exp $"
__version__ = "$Revision: 1.3 $"


from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.New import New \
     as NewMySQL

class New(NewMySQL):
    sql = NewMySQL.sql
    sql_no_input = NewMySQL.sql_no_input
