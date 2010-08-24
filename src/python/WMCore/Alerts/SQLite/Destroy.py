#!/usr/bin/python
"""
_Destroy_

Remove the alert_current and alert_history tables.
"""




from WMCore.Alerts.MySQL.Destroy import Destroy as DestroyMySQL

class Destroy(DestroyMySQL):
    """
    _Destroy_

    alert_current and alert_history tables.
    """