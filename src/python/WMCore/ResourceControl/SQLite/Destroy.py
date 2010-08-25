#/usr/bin/env python
"""
_Destroy_

"""

__revision__ = "$Id: Destroy.py,v 1.1 2009/10/05 20:03:01 mnorman Exp $"
__version__ = "$Revision: 1.1 $"


from WMCore.ResourceControl.MySQL.Destroy import Destroy as MySQLDestroy

class Destroy(MySQLDestroy):
    """
    Identical to MySQL version

    """
