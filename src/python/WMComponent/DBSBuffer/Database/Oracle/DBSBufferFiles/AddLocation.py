#!/usr/bin/env python
"""
_AddLocation_

Oracle implementation of DBSBufferFiles.AddLocation
"""

__revision__ = "$Id: AddLocation.py,v 1.3 2009/10/13 19:40:21 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.AddLocation \
     import AddLocation as MySQLAddLocation

class AddLocation(MySQLAddLocation):
    pass
