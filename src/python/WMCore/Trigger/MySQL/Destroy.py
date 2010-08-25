#!/usr/bin/python
#pylint: disable-msg=E1103

"""
_Create_

Class for destroying MysQL specific schema for the trigger

"""

__revision__ = "$Id: Destroy.py,v 1.3 2009/07/24 17:53:54 mnorman Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "fvlingen@caltech.edu"

import threading

from WMCore.Database.DBCreator import DBCreator

class Destroy(DBCreator):
    """
    _Create_
    
    Class for destroying MysQL specific schema for the trigger
    """
    
    
    
    def __init__(self):
        myThread = threading.currentThread()
        DBCreator.__init__(self, myThread.logger, myThread.dbi)
        self.create = {}
        self.constraints = {}
        self.create['a_tr_trigger'] = """DROP TABLE tr_trigger"""
        self.create['b_tr_trigger'] = """DROP TABLE tr_action"""
