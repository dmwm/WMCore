"""
_New_

SQLite implementation of Block.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.2 2010/08/06 21:05:18 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WorkQueue.Database.MySQL.Data.New import New \
     as NewMySQL

class New(NewMySQL):
    pass
