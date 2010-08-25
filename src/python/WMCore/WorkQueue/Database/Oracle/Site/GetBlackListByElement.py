"""
MySQL implementation of Site.GetBlackListByElement
"""

__all__ = []
__revision__ = "$Id: GetBlackListByElement.py,v 1.1 2009/11/20 22:59:57 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Site.GetBlackListByElement \
     import GetBlackListByElement as GetBlackListByElementMySQL

class GetBlackListByElement(GetBlackListByElementMySQL):
    
    #This query is prerry ugly: find the better way to handle that.
    sql = GetBlackListByElementMySQL.sql
