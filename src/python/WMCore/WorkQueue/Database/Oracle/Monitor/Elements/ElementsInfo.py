"""
_ElementsInfo_

Oracle implementation of Monitor.Elements.ElementsInfo
"""

__all__ = []
__revision__ = "$Id: ElementsInfo.py,v 1.1 2010/06/03 17:07:19 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Monitor.Elements.ElementsInfo \
     import ElementsInfo as ElementsInfoMySQL

class ElementsInfo(ElementsInfoMySQL):
    pass