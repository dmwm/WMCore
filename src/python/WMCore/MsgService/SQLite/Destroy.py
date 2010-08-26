#/usr/bin/env python2.4
"""
_Destroy_

"""

__revision__ = "$Id: Destroy.py,v 1.1 2009/07/20 18:33:23 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import threading

from WMCore.MsgService.MySQL.Destroy import Destroy as MySQLDestroy

class Destroy(MySQLDestroy):
    """
    SQLite implementation of MsgService Destroy

    """
