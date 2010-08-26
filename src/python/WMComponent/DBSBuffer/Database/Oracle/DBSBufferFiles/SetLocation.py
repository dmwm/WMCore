#!/usr/bin/env python
"""
_SetLocation_

Oracle implementation of DBSBufferFiles.SetLocation
"""

__revision__ = "$Id: SetLocation.py,v 1.3 2009/10/22 14:46:43 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.SetLocation import SetLocation as \
     MySQLSetLocation

class SetLocation(MySQLSetLocation):
    pass

                
