#!/usr/bin/env python
"""
_SetLocation_

SQLite implementation of SetFileLocation
"""

__revision__ = "$Id: SetLocation.py,v 1.8 2009/12/16 17:45:42 sfoulkes Exp $"
__version__ = "$Revision: 1.8 $"

from WMCore.WMBS.MySQL.Files.SetLocation import SetLocation as SetLocationMySQL

class SetLocation(SetLocationMySQL):
    pass
        
