#/usr/bin/env python2.4
"""
_Destroy_

"""




import threading

from WMCore.MsgService.MySQL.Destroy import Destroy as MySQLDestroy

class Destroy(MySQLDestroy):
    """
    SQLite implementation of MsgService Destroy

    """
