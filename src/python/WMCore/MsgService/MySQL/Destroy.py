#/usr/bin/env python2.4
"""
_Destroy_

"""

__revision__ = "$Id: Destroy.py,v 1.1 2009/07/13 19:55:56 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import threading

from WMCore.Database.DBCreator import DBCreator

from WMCore.WMException import WMException
from WMCore.WMExceptions import WMEXCEPTION

class Destroy(DBCreator):    
    def __init__(self, logger = None, dbi = None):
        """
        _init_

        Call the DBCreator constructor and create the list of required tables.
        """
        myThread = threading.currentThread()

        if logger == None:
            logger = myThread.logger
        if dbi == None:
            dbi = myThread.dbi
            
        DBCreator.__init__(self, logger, dbi)

        self.create["17ms_type"]                         = "DROP TABLE ms_type"
        self.create["16ms_process"]                      = "DROP TABLE ms_process"
        self.create["15ms_history"]                      = "DROP TABLE ms_history"
        self.create["14ms_history_buffer"]               = "DROP TABLE ms_history_buffer"
        self.create["13ms_history_priority"]             = "DROP TABLE ms_history_priority"
        self.create["12ms_history_priority_buffer"]      = "DROP TABLE ms_history_priority_buffer"
        self.create["11ms_message"]                      = "DROP TABLE ms_message"
        self.create["10ms_message_buffer_in"]            = "DROP TABLE ms_message_buffer_in"
        self.create["09ms_message_buffer_out"]           = "DROP TABLE ms_message_buffer_out"
        self.create["08ms_priority_message"]             = "DROP TABLE ms_priority_message"
        self.create["07ms_priority_message_buffer_in"]   = "DROP TABLE ms_priority_message_buffer_in"
        self.create["06ms_priority_message_buffer_out"]  = "DROP TABLE ms_priority_message_buffer_out"
        self.create["05ms_subscription"]                 = "DROP TABLE ms_subscription"
        self.create["04ms_subscription_priority"]        = "DROP TABLE ms_subscription_priority"
        self.create["03ms_available"]                    = "DROP TABLE ms_available"
        self.create["02ms_available_priority"]           = "DROP TABLE ms_available_priority"
        self.create["01ms_check_buffer"]                 = "DROP TABLE ms_check_buffer"

