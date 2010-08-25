"""
_LoadByBlockID_

Oracle implementation of Site.LoadByBlockID
"""

__all__ = []
__revision__ = "$Id: LoadByDataID.py,v 1.1 2010/04/07 19:17:18 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Site.LoadByDataID import LoadByDataID \
     as LoadByDataIDMySQL

class LoadByDataID(LoadByDataIDMySQL):
    sql = LoadByDataIDMySQL.sql
