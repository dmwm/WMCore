"""
_New_

Oracle implementation of WMSpec.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.1 2009/06/25 18:55:52 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.WMSpec.New import New \
     as NewMySQL
     
class New(NewMySQL):
    sql = NewMySQL.sql
    