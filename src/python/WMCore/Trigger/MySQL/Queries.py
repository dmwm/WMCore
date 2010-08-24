#!/usr/bin/env python
#pylint: disable-msg=E1103
"""
_Queries_

This module implements the mysql backend for the trigger.

"""

__revision__ = \
    "$Id: Queries.py,v 1.2 2008/09/09 13:50:36 fvlingen Exp $"
__version__ = \
    "$Revision: 1.2 $"
__author__ = \
    "fvlingen@caltech.edu"

import threading

from WMCore.Database.DBFormatter import DBFormatter


class Queries(DBFormatter):
    """
    _Queries_
    
    This module implements the mysql backend for the trigger.
    
    """
    
    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)
        # size we use for bulk inserts and deletes until binding is fixed.
        self.size = 200

    def lockTrigger(self, args):
        """
        Locks a row in the table to prevent concurrent writes.
        """
        sqlStr = """
SELECT * FROM tr_trigger WHERE trigger_id = :trigger_id AND id = :id FOR UPDATE
        """
        self.execute(sqlStr, args) 

    def insertFlag(self, args):
        """
        Inserts a flag(s) into the trigger table.
        """
        sqlStr = """
INSERT INTO tr_trigger(id,flag_id,trigger_id) VALUES(:id,:flag_id,:trigger_id)
        """
        self.execute(sqlStr, args)
        

    def setAction(self, args):
        """
        Sets the associated action for a trigger. If the action was set it 
        updates it.
        """
        sqlStr = """
INSERT INTO tr_action(id,trigger_id,action_name,payload) VALUES(:id,:trigger_id,:action_name,:payload) 
ON DUPLICATE KEY UPDATE action_name = VALUES(action_name)
        """
        self.execute(sqlStr, args)        

    def removeFlag(self, args):
        """
        removes the flags (which is done when a flag is set)
        . Once all flags are finished (thus removed).
        the associated action is activated.
        """
        if len(args) == 0:
            return 
        if len(args) == 1:
            # a bind for one works
            sqlStr = """
DELETE FROM tr_trigger WHERE trigger_id = :trigger_id AND flag_id = :flag_id AND id = :id
            """
            self.execute(sqlStr, args)
        else:
            # reformat query to do delete without binds
            # FIXME: there are some problems with multi bind delete.
            # also there is a maximum query size mysql can deal with.
            start = 0
            end = self.size
            while start < len(args):
                if end > len(args):
                    end = len(args)
                sqlStr = """
DELETE FROM tr_trigger WHERE 
                """
                orOp = False
                for arg in args[start:end]:
                    if orOp:
                        sqlStr += ' OR '
                    orOp = True
                    sqlStr += " (trigger_id='"+str(arg['trigger_id'])+\
                        "' AND flag_id='"+str(arg['flag_id'])+"')"
                self.execute(sqlStr, {})
                start += 100
                end += 100


    def allFlagsSet(self, args):
        """
        Builds Query that checks if all flags for a trigger are set.
        """
        if len(args) == 0:
            return 
        if len(args) == 1:
            sqlStr = """
SELECT COUNT(*) as total_count,trigger_id, id FROM tr_trigger WHERE 
trigger_id = :trigger_id AND id = :id GROUP BY trigger_id,id 
        """             
            result = self.execute(sqlStr, args)
            return self.formatDict(result) 
        else:
            # reformat query to do delete without binds
            # FIXME: there are some problems with multi bind 
            sqlStr = """
SELECT COUNT(*) as total_count,trigger_id, id FROM tr_trigger WHERE 
            """
            orOp = False
            for arg in args:
                if orOp:
                    sqlStr += ' OR '
                orOp = True
                sqlStr += " (trigger_id='"+str(arg['trigger_id'])+\
                    "' AND id='"+str(arg['id'])+"')"
            sqlStr += " GROUP BY trigger_id,id "
            result = self.execute(sqlStr, {})
            return self.format(result) 


    def selectAction(self, args):
        """
        Once the trigger is triggered, this query selects an action to be loaded
        and executed.
        """
        if type(args) != list:
            args = [args]
        if len(args) == 0:
            return
        else:
            orOps = False
            sqlStr1 = """
SELECT action_name,trigger_id,id, payload FROM tr_action WHERE 
            """
            sqlStr2 = "DELETE FROM tr_action WHERE "
            for arg in args:
                if orOps :
                    sqlStr1 += " OR "
                    sqlStr2 += " OR "
                sqlStr1 += """(trigger_id='%s' AND id='%s') 
                """ % (arg['trigger_id'], arg['id'])
                sqlStr2 += """(trigger_id='%s' AND id='%s') 
                """ % (arg['trigger_id'], arg['id'])
                orOps = True
            result = self.execute(sqlStr1, {})
            result = self.formatDict(result) 
            self.execute(sqlStr2, {})
            return result

    def createTriggerTables(self, triggerName):
        trigger = "tr_trigger_"+triggerName
        sqlStr1 = """
CREATE TABLE %s(
   id VARCHAR(32) NOT NULL,
   trigger_id VARCHAR(32) NOT NULL,
   flag_id VARCHAR(32) NOT NULL,
   time timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
   UNIQUE(id,trigger_id,flag_id),
   INDEX(trigger_id)
   ) TYPE=InnoDB;
        """ %trigger
        action = "tr_action_"+triggerName
        sqlStr2 = """
CREATE TABLE %s(
   id VARCHAR(32) NOT NULL,
   trigger_id VARCHAR(32) NOT NULL,
   /* Action name associated to this trigger. This name
   is associated to some python code in an action registery
   */
   action_name VARCHAR(255) NOT NULL,
   payload text,
   UNIQUE(id,trigger_id)
   ) TYPE=InnoDB;
        """ %action
        self.execute(sqlStr1, {})
        self.execute(sqlStr2, {})





    def execute(self, sqlStr, args):
        """"
        __execute__
        Executes the queries by getting the current transaction
        and dbinterface object that is stored in the reserved words of
        the thread it operates in.
        """
        myThread = threading.currentThread()
        currentTransaction = myThread.transaction
        return currentTransaction.processData(sqlStr, args) 
