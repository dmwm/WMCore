#!/usr/bin/python
#pylint: disable-msg=E1103

"""
_Create_

Class for creating MySQL specific schema for the trigger

"""

__revision__ = "$Id: Create.py,v 1.1 2008/09/19 15:34:35 fvlingen Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "fvlingen@caltech.edu"

import threading

from WMCore.Database.DBCreator import DBCreator

class Create(DBCreator):
    """
    _Create_
    
    Class for creating MySQL specific schema for the trigger.
    """
    
    
    
    def __init__(self):
        myThread = threading.currentThread()
        DBCreator.__init__(self, myThread.logger, myThread.dbi)
        self.create = {}
        self.constraints = {}
        self.create['a_tr_trigger'] = """
CREATE TABLE tr_action
(	id VARCHAR2(32 BYTE) NOT NULL ENABLE, 
	trigger_id VARCHAR2(32 BYTE) NOT NULL ENABLE, 
	action_name VARCHAR2(255 BYTE) NOT NULL ENABLE, 
	payload CLOB NOT NULL ENABLE, 
	 CONSTRAINT TR_ACTION_UK1 UNIQUE (id, trigger_id, action_name)
)
"""
        self.create['b_tr_action'] = """
CREATE TABLE tr_trigger 
(	id VARCHAR2(32 BYTE) NOT NULL ENABLE, 
	trigger_id VARCHAR2(32 BYTE) NOT NULL ENABLE, 
	flag_id VARCHAR2(32 BYTE) NOT NULL ENABLE, 
	 CONSTRAINT TR_TRIGGER_PK PRIMARY KEY (id, trigger_id, flag_id)
)  
"""
