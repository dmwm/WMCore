"""
MySQL implementation of Site.GetWhiteListByElement
"""

__all__ = []
__revision__ = "$Id: GetWhiteListByElement.py,v 1.1 2009/11/20 22:59:57 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Site.GetWhiteListByElement \
     import GetWhiteListByElement as GetWhiteListByElementMySQL


class GetWhiteListByElement(GetWhiteListByElementMySQL):
    
    #This query is prerry ugly: find the better way to handle that.
    sql = GetWhiteListByElementMySQL.sql
