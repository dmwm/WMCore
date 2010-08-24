"""
QLite implementation of Site.CheckValidity
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Site.CheckValidity import CheckValidity \
     as CheckValidityMySQL

class CheckValidity(CheckValidityMySQL):
    
    #This query is prerry ugly: find the better way to handle that.
    sql = CheckValidityMySQL.sql