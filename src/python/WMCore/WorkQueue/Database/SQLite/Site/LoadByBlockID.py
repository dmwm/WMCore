"""
_New_

SQLite implementation of site.LoadByBlockID
"""

__all__ = []
__revision__ = "$Id: LoadByBlockID.py,v 1.1 2009/08/18 23:18:16 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Site.LoadByBlockID import LoadByBlockID \
     as LoadByBlockIDMySQL

class AddParent(LoadByBlockIDMySQL):
    sql = LoadByBlockIDMySQL.sql
