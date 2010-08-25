"""
_LoadByBlockID_

Oracle implementation of Site.LoadByBlockID
"""

__all__ = []
__revision__ = "$Id: LoadByBlockID.py,v 1.1 2009/08/27 21:04:30 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Site.LoadByBlockID import LoadByBlockID \
     as LoadByBlockIDMySQL

class AddParent(LoadByBlockIDMySQL):
    sql = LoadByBlockIDMySQL.sql
