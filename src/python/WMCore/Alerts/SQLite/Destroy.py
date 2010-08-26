#!/usr/bin/python
"""
_Destroy_

Remove the alert_current and alert_history tables.
"""

__revision__ = "$Id: Destroy.py,v 1.1 2009/07/10 21:45:34 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Alerts.MySQL.Destroy import Destroy as DestroyMySQL

class Destroy(DestroyMySQL):
    """
    _Destroy_

    alert_current and alert_history tables.
    """