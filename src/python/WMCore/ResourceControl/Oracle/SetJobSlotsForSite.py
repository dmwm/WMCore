#!/usr/bin/env python
"""
_SetJobSlotsForSite_

Oracle implementation of ResourceControl.SetJobSlotsForSite
"""

__revision__ = "$Id: SetJobSlotsForSite.py,v 1.1 2010/07/15 16:57:06 sfoulkes Exp $"
__version__  = "$Revision: 1.1 $"

from WMCore.ResourceControl.MySQL.SetJobSlotsForSite import SetJobSlotsForSite as MySQLSetJobSlotsForSite

class SetJobSlotsForSite(MySQLSetJobSlotsForSite):
    pass
