"""
_ElementsById_

Oracle implementation of Monitor.Elements.ElementsById
"""

__all__ = []
__revision__ = "$Id: ElementsById.py,v 1.1 2010/06/03 17:07:19 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Monitor.Elements.ElementsById \
     import ElementsById as ElementsByIdMySQL

class ElementsById(ElementsByIdMySQL):
    pass