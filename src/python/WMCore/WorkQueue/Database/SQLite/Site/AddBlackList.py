"""
_AddBlackList_

MySQL implementation of Site.AddBlackList
"""

__all__ = []
__revision__ = "$Id: AddBlackList.py,v 1.1 2009/11/20 23:00:00 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Site.AddBlackList import AddBlackList \
     as AddBlackListMySQL

class AddBlackList(AddBlackListMySQL):

    sql = AddBlackListMySQL.sql.replace('IGNORE', 'OR IGNORE')
