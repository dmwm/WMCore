"""
MySQL implementation of Site.GetWhiteListByElement
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Site.GetWhiteListByElement \
     import GetWhiteListByElement as GetWhiteListByElementMySQL


class GetWhiteListByElement(GetWhiteListByElementMySQL):
    
    #This query is prerry ugly: find the better way to handle that.
    sql = GetWhiteListByElementMySQL.sql
