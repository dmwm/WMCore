"""
_ElementsByState_

Oracle implementation of Monitor.Elements.ElementsByState
"""

__all__ = []
__revision__ = "$Id: ElementsByState.py,v 1.1 2010/06/03 17:07:19 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Monitor.Elements.ElementsByState \
     import ElementsByState as ElementsByStateMySQL

class ElementsByState(ElementsByStateMySQL):
    pass