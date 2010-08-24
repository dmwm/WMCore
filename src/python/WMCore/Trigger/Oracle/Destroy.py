#!/usr/bin/python
#pylint: disable-msg=E1103

"""
_Create_

Class for creating MySQL specific schema for the trigger

"""

__revision__ = "$Id: Destroy.py,v 1.1 2008/09/19 15:34:35 fvlingen Exp $"
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
        self.create['a_tr_trigger'] = """DROP TABLE tr_trigger """
        self.create['b_tr_action'] = """DROP TABLE tr_action """
