#!/usr/bin/env python
"""
_HeritageLFNParent_

Oracle implementation of DBSBufferFiles.HeritageLFNParent
"""

__revision__ = "$Id: HeritageLFNParent.py,v 1.1 2009/10/22 15:40:20 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.HeritageLFNParent import \
     HeritageLFNParent as MySQLHeritageLFNParent

class HeritageLFNParent(MySQLHeritageLFNParent):
    pass
