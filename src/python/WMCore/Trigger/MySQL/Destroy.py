#!/usr/bin/python
#pylint: disable-msg=E1103

"""
_Create_

Class for destroying MysQL specific schema for the trigger

"""





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
