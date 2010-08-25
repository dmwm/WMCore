"""
_InsertComponent_

Oracle implementation of InsertComponent
"""

__all__ = []
__revision__ = "$Id: InsertComponent.py,v 1.1 2010/06/21 21:18:09 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Agent.Database.MySQL.InsertComponent import InsertComponent \
     as InsertComponentMySQL

class InsertComponent(InsertComponentMySQL):
    pass
