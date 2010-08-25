"""
_ElementsById_

Oracle implementation of Monitor.Elements.ElementsById
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Monitor.Elements.ElementsById \
     import ElementsById as ElementsByIdMySQL

class ElementsById(ElementsByIdMySQL):
    pass