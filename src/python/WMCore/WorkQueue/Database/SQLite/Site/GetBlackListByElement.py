"""
MySQL implementation of Site.GetBlackListByElement
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Site.GetBlackListByElement \
     import GetBlackListByElement as GetBlackListByElementMySQL

class GetBlackListByElement(GetBlackListByElementMySQL):
    
    #This query is prerry ugly: find the better way to handle that.
    sql = GetBlackListByElementMySQL.sql
