"""
_CountElements_

Oracle implementation of WMSpec.CountElements
"""
__all__ = []



from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.CountElements import CountElements \
     as CountElementsMySQL

class CountElements(CountElementsMySQL):
    """
    same as MySql implementation
    """
