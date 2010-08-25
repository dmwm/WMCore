#!/usr/bin/python
"""
_Destroy_

Remove the alert_current and alert_history tables.
"""

__revision__ = "$Id: Destroy.py,v 1.1 2009/07/10 21:45:34 sryu Exp $"
__version__ = "$Revision: 1.1 $"

import threading

from WMCore.Alerts.MySQL.Destroy import Destroy as DestroyMySQL

class Destroy(DestroyMySQL):
    """
    _Destroy_

    Remove the alert_current and alert_history tables.
    """
    def __init__(self):
        
        DestroyMySQL.__init__(self)
        self.create["alert_current_SEQ"] = "DROP SEQUENCE alert_current_SEQ"
        self.create["alert_history_SEQ"] = "DROP SEQUENCE alert_history_SEQ"
        
        # 00 prefix trigger has to be dropped first than sequence
        # Or remove the followings - It seems when sequence gets dropped trigger automatically dropped as well
        self.create["00alert_current_TRG"] = "DROP TRIGGER alert_current_TRG"
        self.create["00alert_history_TRG"] = "DROP TRIGGER alert_history_TRG"
        
