"""
_AddBlackList_

MySQL implementation of Site.AddBlackList
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Site.AddBlackList import AddBlackList \
     as AddBlackListMySQL

class AddBlackList(AddBlackListMySQL):

    sql = AddBlackListMySQL.sql.replace('IGNORE', 'OR IGNORE')
