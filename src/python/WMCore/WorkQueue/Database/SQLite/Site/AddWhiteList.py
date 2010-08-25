"""
_AddWhiteList_

MySQL implementation of Site.AddWhiteList
"""

__all__ = []
__revision__ = "$Id: AddWhiteList.py,v 1.1 2009/11/20 23:00:00 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Site.AddWhiteList import AddWhiteList \
     as AddWhiteListMySQL

class AddWhiteList(AddWhiteListMySQL):
    
    sql = AddWhiteListMySQL.sql.replace('IGNORE', 'OR IGNORE')
             