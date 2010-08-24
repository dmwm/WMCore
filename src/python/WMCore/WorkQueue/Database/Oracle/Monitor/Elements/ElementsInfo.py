"""
_ElementsInfo_

Oracle implementation of Monitor.Elements.ElementsInfo
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Monitor.Elements.ElementsInfo \
     import ElementsInfo as ElementsInfoMySQL

class ElementsInfo(ElementsInfoMySQL):
    pass