"""
_New_

SQLite implementation of WMSpec.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.1 2009/07/17 14:25:29 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.WMSpec.New import New \
     as NewMySQL
     
class New(NewMySQL):
    sql = NewMySQL.sql
    