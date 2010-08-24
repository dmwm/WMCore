#!/usr/bin/python
#pylint: disable-msg=E1103

"""
_Create_

Class for creating MySQL specific schema for the trigger

"""

__revision__ = "$Id: Destroy.py,v 1.1 2008/10/02 11:33:03 fvlingen Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "fvlingen@caltech.edu"

import threading

from WMCore.Database.DBCreator import DBCreator

class Destroy(DBCreator):
    """
    _Create_
    
    Class for creating MySQL specific schema for the trigger.
    """
    
    
    
    def __init__(self):
        myThread = threading.currentThread()
        DBCreator.__init__(self, myThread.logger, myThread.dbi)
        self.create = {}
        self.constraints = {}
        self.create['z_ms_process'] = "DROP TABLE ms_process"
        self.create['y_ms_process_seq'] = "DROP SEQUENCE MS_PROCESS_SEQ1"
        self.create['x_ms_type'] = "DROP TABLE ms_type"
        self.create['w_ms_type_seq'] = "DROP SEQUENCE MS_TYPE_SEQ1"
