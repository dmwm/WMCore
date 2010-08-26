#!/usr/bin/python
#pylint: disable-msg=E1103

"""
_Create_

Class for creating SQLite specific schema for the trigger

"""

__revision__ = "$Id: Create.py,v 1.2 2009/07/23 14:06:42 mnorman Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "mnorman@fnal.gov"

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
   id         VARCHAR(32) NOT NULL,
   trigger_id VARCHAR(32) NOT NULL,
   flag_id    VARCHAR(32) NOT NULL,
   time       timestamp   NOT NULL    default CURRENT_TIMESTAMP,
   UNIQUE(id,trigger_id,flag_id)
   );
"""
        self.create['b_tr_trigger'] = """
CREATE TABLE tr_action(
   id          VARCHAR(32) NOT NULL,
   trigger_id  VARCHAR(32) NOT NULL,
   /* Action name associated to this trigger. This name
   is associated to some python code in an action registery
   */
   action_name VARCHAR(255) NOT NULL,
   payload     text,
   UNIQUE(id,trigger_id)
   );
"""

        #Have to create a trigger because SQLite doesn't do "on update"
        self.create['a_tr_trigger_trigger'] = """
CREATE TRIGGER tr_trigger_trigger BEFORE UPDATE ON tr_trigger
        FOR EACH ROW
        BEGIN
             UPDATE tr_triger SET time = CURRENT_TIMESTAMP WHERE trigger_id = NEW.trigger_id;
        END;

        """
        
 
