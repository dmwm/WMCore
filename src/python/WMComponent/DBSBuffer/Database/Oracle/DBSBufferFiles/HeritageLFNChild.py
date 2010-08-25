#!/usr/bin/env python
"""
_HeritageLFNChild_

Oracle implementation of DBSBufferFiles.HeritageLFNChild
"""

__revision__ = "$Id: HeritageLFNChild.py,v 1.1 2009/10/22 15:40:20 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.HeritageLFNChild import \
     HeritageLFNChild as MySQLHeritageLFNChild

class HeritageLFNChild(MySQLHeritageLFNChild):
    pass
