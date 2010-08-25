"""
_CountElements_

SQLite implementation of WMSpec.CountElements
"""
__all__ = []



from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.CountElements import CountElements \
     as CountElementsMySQL

class CountElements(CountElementsMySQL):
    """
    same as MySql implementation
    """
