#/usr/bin/env python2.4
"""
_Destroy_

"""

__revision__ = "$Id: Destroy.py,v 1.2 2009/08/12 17:22:40 meloam Exp $"
__version__ = "$Revision: 1.2 $"

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
        
        self.delete["17ms_type"]                         = "DROP TABLE ms_type"
        self.delete["16ms_process"]                      = "DROP TABLE ms_process"
        self.delete["15ms_history"]                      = "DROP TABLE ms_history"
        self.delete["14ms_history_buffer"]               = "DROP TABLE ms_history_buffer"
        self.delete["13ms_history_priority"]             = "DROP TABLE ms_history_priority"
        self.delete["12ms_history_priority_buffer"]      = "DROP TABLE ms_history_priority_buffer"
        self.delete["11ms_message"]                      = "DROP TABLE ms_message"
        self.delete["10ms_message_buffer_in"]            = "DROP TABLE ms_message_buffer_in"
        self.delete["09ms_message_buffer_out"]           = "DROP TABLE ms_message_buffer_out"
        self.delete["08ms_priority_message"]             = "DROP TABLE ms_priority_message"
        self.delete["07ms_priority_message_buffer_in"]   = "DROP TABLE ms_priority_message_buffer_in"
        self.delete["06ms_priority_message_buffer_out"]  = "DROP TABLE ms_priority_message_buffer_out"
        self.delete["05ms_subscription"]                 = "DROP TABLE ms_subscription"
        self.delete["04ms_subscription_priority"]        = "DROP TABLE ms_subscription_priority"
        self.delete["03ms_available"]                    = "DROP TABLE ms_available"
        self.delete["02ms_available_priority"]           = "DROP TABLE ms_available_priority"
        self.delete["01ms_check_buffer"]                 = "DROP TABLE ms_check_buffer"

