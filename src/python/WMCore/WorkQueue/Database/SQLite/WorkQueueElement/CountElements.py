"""
_Exists_

SQLite implementation of WMSpec.CountElements
"""
__all__ = []
__revision__ = "$Id: CountElements.py,v 1.1 2009/11/17 16:53:32 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.CountElements import CountElements \
     as CountElementsMySQL

class CountElements(CountElementsMySQL):
    #sql = CountElementsMySQL.sql
    pass
