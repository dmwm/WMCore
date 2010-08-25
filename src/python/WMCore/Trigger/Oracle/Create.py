#!/usr/bin/python
#pylint: disable-msg=E1103

"""
_Create_

Class for creating MySQL specific schema for the trigger

"""

__revision__ = "$Id: Create.py,v 1.2 2009/08/31 19:12:40 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

import threading

from WMCore.Database.DBCreator import DBCreator

class Create(DBCreator):
    """
    _Create_
    
    Class for creating MySQL specific schema for the trigger.
    """
    def __init__(self, logger = None, dbi = None, params = None):
        myThread = threading.currentThread()
        DBCreator.__init__(self, myThread.logger, myThread.dbi)
        self.create = {}
        self.constraints = {}

        tablespaceTable = ""
        tablespaceIndex = ""

        if params:
            if params.has_key("tablespace_table"):
                tablespaceTable = "TABLESPACE %s" % params["tablespace_table"]
            if params.has_key("tablespace_index"):
                tablespaceIndex = "USING INDEX TABLESPACE %s" % params["tablespace_index"]
                                                            
        self.create["01_tr_action"] = \
          """CREATE TABLE tr_action (
               id          VARCHAR2(32 BYTE)  NOT NULL ENABLE, 
	       trigger_id  VARCHAR2(32 BYTE)  NOT NULL ENABLE, 
	       action_name VARCHAR2(255 BYTE) NOT NULL ENABLE, 
	       payload     CLOB               NOT NULL ENABLE 
               ) %s""" % tablespaceTable

        self.indexes["01_pk_tr_action"] = \
          """ALTER TABLE tr_action ADD                            
	       (CONSTRAINT tr_action_pk PRIMARY KEY (id, trigger_id, action_name) %s)""" % tablespaceIndex

        self.create["01_tr_trigger"] = \
          """CREATE TABLE tr_trigger (
               id         VARCHAR2(32 BYTE) NOT NULL ENABLE, 
	       trigger_id VARCHAR2(32 BYTE) NOT NULL ENABLE, 
	       flag_id    VARCHAR2(32 BYTE) NOT NULL ENABLE
               ) %s""" % tablespaceTable

        self.indexes["01_pk_tr_action"] = \
          """ALTER TABLE tr_action ADD
               (CONSTRAINT TR_TRIGGER_PK PRIMARY KEY (id, trigger_id, flag_id) %s)""" % tablespaceIndex
