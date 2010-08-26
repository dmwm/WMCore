#!/usr/bin/env python
"""
_Save_

Oracle implementation of Jobs.Save
"""

__all__ = []
__revision__ = "$Id: Save.py,v 1.4 2009/05/18 17:31:41 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMBS.MySQL.Jobs.Save import Save as SaveJobMySQL

class Save(SaveJobMySQL):
    pass
