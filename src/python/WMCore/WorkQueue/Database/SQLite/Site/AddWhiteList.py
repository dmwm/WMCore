"""
_AddWhiteList_

MySQL implementation of Site.AddWhiteList
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Site.AddWhiteList import AddWhiteList \
     as AddWhiteListMySQL

class AddWhiteList(AddWhiteListMySQL):
    
    sql = AddWhiteListMySQL.sql.replace('IGNORE', 'OR IGNORE')
             