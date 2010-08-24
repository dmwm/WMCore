#!/usr/bin/python
#pylint: disable-msg=E1103

"""
_Create_

Class for creating MySQL specific schema for the trigger

"""

__revision__ = "$Id: Create.py,v 1.2 2008/09/09 13:50:36 fvlingen Exp $"
__version__ = "$Revision: 1.2 $"
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
CREATE TABLE tr_trigger(
   id VARCHAR(32) NOT NULL,
   trigger_id VARCHAR(32) NOT NULL,
   flag_id VARCHAR(32) NOT NULL,
   time timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
   UNIQUE(id,trigger_id,flag_id),
   INDEX(trigger_id)
   ) TYPE=InnoDB;
"""
        self.create['b_tr_trigger'] = """
CREATE TABLE tr_action(
   id VARCHAR(32) NOT NULL,
   trigger_id VARCHAR(32) NOT NULL,
   /* Action name associated to this trigger. This name
   is associated to some python code in an action registery
   */
   action_name VARCHAR(255) NOT NULL,
   payload text,
   UNIQUE(id,trigger_id)
   ) TYPE=InnoDB;
"""
 
