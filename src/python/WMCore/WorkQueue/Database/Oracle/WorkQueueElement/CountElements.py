"""
_CountElements_

Oracle implementation of WMSpec.CountElements
"""
__all__ = []
__revision__ = "$Id: CountElements.py,v 1.1 2009/11/20 23:00:01 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.CountElements import CountElements \
     as CountElementsMySQL

class CountElements(CountElementsMySQL):
    """
    same as MySql implementation
    """
