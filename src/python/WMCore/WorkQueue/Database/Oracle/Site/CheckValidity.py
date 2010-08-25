"""
QLite implementation of Site.CheckValidity
"""

__all__ = []
__revision__ = "$Id: CheckValidity.py,v 1.1 2009/11/20 22:59:57 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Site.CheckValidity import CheckValidity \
     as CheckValidityMySQL

class CheckValidity(CheckValidityMySQL):
    
    #This query is prerry ugly: find the better way to handle that.
    sql = CheckValidityMySQL.sql