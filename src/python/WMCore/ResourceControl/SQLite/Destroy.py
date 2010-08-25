#/usr/bin/env python
"""
_Destroy_

SQLite implementation of ResourceControl.Destroy.
"""

__revision__ = "$Id: Destroy.py,v 1.2 2010/02/09 17:59:15 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.ResourceControl.MySQL.Destroy import Destroy as MySQLDestroy

class Destroy(MySQLDestroy):
    pass
