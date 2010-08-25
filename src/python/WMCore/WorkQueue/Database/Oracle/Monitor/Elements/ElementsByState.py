"""
_ElementsByState_

Oracle implementation of Monitor.Elements.ElementsByState
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Monitor.Elements.ElementsByState \
     import ElementsByState as ElementsByStateMySQL

class ElementsByState(ElementsByStateMySQL):
    pass