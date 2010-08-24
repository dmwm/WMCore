#!/usr/bin/python
#pylint: disable-msg=E1103

"""
_Create_

Class for destroying MysQL specific schema for the trigger

"""

__revision__ = "$Id: Destroy.py,v 1.2 2008/09/26 14:48:04 fvlingen Exp $"
__version__ = "$Revision: 1.2 $"
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
