#!/usr/bin/env python
"""
_Create_

Class for creating SQLite specific schema for persistent messages.
"""

__revision__ = "$Id: Create.py,v 1.6 2010/02/09 17:33:23 meloam Exp $"
__version__ = "$Revision: 1.6 $"

import logging
import threading

from WMCore.Database.DBCreator import DBCreator

class Create(DBCreator):
    """
    _Create_
    
    Class for creating SQLite specific schema for persistent messages.
    """
    
    
    
    def __init__(self, logger=None, dbi=None, params = None):
        myThread = threading.currentThread()

        if logger == None:
            logger = myThread.logger
        if dbi == None:
            dbi = myThread.dbi
            
        DBCreator.__init__(self, logger, dbi)
        self.create = {}
        self.constraints = {}

        self.create['00ta_ms_type'] = """      CREATE TABLE ms_type
        (
             typeid INTEGER PRIMARY KEY AUTOINCREMENT,
             name varchar(255) NOT NULL default '',
             UNIQUE (name)
        )
"""

        self.create['01tb_ms_process'] = """
CREATE TABLE ms_process (
   procid INTEGER PRIMARY KEY AUTOINCREMENT,
   name varchar(255) NOT NULL default '',
   host varchar(255) NOT NULL default '',
   pid int(11) NOT NULL default '0',
   UNIQUE (name)
   ) 
"""
        
        self.create['02tc_ms_history'] = """
CREATE TABLE ms_history (
    messageid INTEGER PRIMARY KEY AUTOINCREMENT,
    type int(11) NOT NULL default '0',
    source int(11) NOT NULL default '0',
    dest int(11) NOT NULL default '0',
    payload text NOT NULL,
    time timestamp NOT NULL default CURRENT_TIMESTAMP,
    delay varchar(50) NOT NULL default '00:00:00',

    FOREIGN KEY(type) references ms_type(typeid),
    FOREIGN KEY(source) references ms_process(procid),
    FOREIGN KEY(dest) references ms_process(procid)
    ) 
"""

        
        self.create['03tca_ms_history_buffer'] = """
CREATE TABLE ms_history_buffer (
    messageid INTEGER PRIMARY KEY AUTOINCREMENT,
    type int(11) NOT NULL default '0',
    source int(11) NOT NULL default '0',
    dest int(11) NOT NULL default '0',
    payload text NOT NULL,
    time timestamp NOT NULL default CURRENT_TIMESTAMP ,
    delay varchar(50) NOT NULL default '00:00:00',

    FOREIGN KEY(type) references ms_type(typeid),
    FOREIGN KEY(source) references ms_process(procid),
    FOREIGN KEY(dest) references ms_process(procid)
    ) 
"""
        self.create['04td_ms_history_priority'] = """
CREATE TABLE ms_history_priority (
   messageid INTEGER PRIMARY KEY AUTOINCREMENT,
   type int(11) NOT NULL default '0',
   source int(11) NOT NULL default '0',
   dest int(11) NOT NULL default '0',
   payload text NOT NULL,
   time timestamp NOT NULL default CURRENT_TIMESTAMP ,
   delay varchar(50) NOT NULL default '00:00:00',

    FOREIGN KEY(type) references ms_type(typeid),
    FOREIGN KEY(source) references ms_process(procid),
    FOREIGN KEY(dest) references ms_process(procid)
    ) 
"""
        self.create['05tda_ms_history_priority_buffer'] = """
CREATE TABLE ms_history_priority_buffer (
   messageid INTEGER PRIMARY KEY AUTOINCREMENT,
   type int(11) NOT NULL default '0',
   source int(11) NOT NULL default '0',
   dest int(11) NOT NULL default '0',
   payload text NOT NULL,
   time timestamp NOT NULL default CURRENT_TIMESTAMP ,
   delay varchar(50) NOT NULL default '00:00:00',

    FOREIGN KEY(type) references ms_type(typeid),
    FOREIGN KEY(source) references ms_process(procid),
    FOREIGN KEY(dest) references ms_process(procid)
    ) 
"""
        
        self.create['06te_ms_message'] = """
CREATE TABLE ms_message (
   messageid INTEGER PRIMARY KEY AUTOINCREMENT,
   type int(11) NOT NULL default '0',
   source int(11) NOT NULL default '0',
   dest int(11) NOT NULL default '0',
   payload text NOT NULL,
   time timestamp NOT NULL default CURRENT_TIMESTAMP ,
   delay varchar(50) NOT NULL default '00:00:00',

   FOREIGN KEY(type) references ms_type(typeid),
   FOREIGN KEY(source) references ms_process(procid),
   FOREIGN KEY(dest) references ms_process(procid)
   ) 
"""
        self.create['07tf_ms_message_buffer_in'] = """
CREATE TABLE ms_message_buffer_in (
   messageid INTEGER PRIMARY KEY AUTOINCREMENT,
   type int(11) NOT NULL default '0',
   source int(11) NOT NULL default '0',
   dest int(11) NOT NULL default '0',
   payload text NOT NULL,
   time timestamp NOT NULL default CURRENT_TIMESTAMP ,
   delay varchar(50) NOT NULL default '00:00:00',

   FOREIGN KEY(type) references ms_type(typeid),
   FOREIGN KEY(source) references ms_process(procid),
   FOREIGN KEY(dest) references ms_process(procid)
   ) 
   """
       
        
        self.create['09tg_ms_message_buffer_out'] = """ 
CREATE TABLE ms_message_buffer_out (
   messageid INTEGER PRIMARY KEY AUTOINCREMENT,
   type int(11) NOT NULL default '0',
   source int(11) NOT NULL default '0',
   dest int(11) NOT NULL default '0',
   payload text NOT NULL,
   time timestamp NOT NULL default CURRENT_TIMESTAMP ,
   delay varchar(50) NOT NULL default '00:00:00',
   state varchar(20) NOT NULL default 'wait',

   FOREIGN KEY(type) references ms_type(typeid),
   FOREIGN KEY(source) references ms_process(procid),
   FOREIGN KEY(dest) references ms_process(procid)
)  
"""

        self.create['10th_ms_priorty_message'] = """
CREATE TABLE ms_priority_message (
   messageid INTEGER PRIMARY KEY AUTOINCREMENT,
   type int(11) NOT NULL default '0',
   source int(11) NOT NULL default '0',
   dest int(11) NOT NULL default '0',
   payload text NOT NULL,
   time timestamp NOT NULL default CURRENT_TIMESTAMP ,
   delay varchar(50) NOT NULL default '00:00:00',

   FOREIGN KEY(type) references ms_type(typeid),
   FOREIGN KEY(source) references ms_process(procid),
   FOREIGN KEY(dest) references ms_process(procid)
   ) 
"""
        self.create['11ti_ms_priority_message_buffer_in'] = """
CREATE TABLE ms_priority_message_buffer_in (
   messageid INTEGER PRIMARY KEY AUTOINCREMENT,
   type int(11) NOT NULL default '0',
   source int(11) NOT NULL default '0',
   dest int(11) NOT NULL default '0',
   payload text NOT NULL,
   time timestamp NOT NULL default CURRENT_TIMESTAMP ,
   delay varchar(50) NOT NULL default '00:00:00',

   FOREIGN KEY(type) references ms_type(typeid),
   FOREIGN KEY(source) references ms_process(procid),
   FOREIGN KEY(dest) references ms_process(procid)
   ) 
"""
        self.create['12tj_ms_priority_message_buffer_out'] = """
CREATE TABLE ms_priority_message_buffer_out (
   messageid INTEGER PRIMARY KEY AUTOINCREMENT,
   type int(11) NOT NULL default '0',
   source int(11) NOT NULL default '0',
   dest int(11) NOT NULL default '0',
   payload text NOT NULL,
   time timestamp NOT NULL default CURRENT_TIMESTAMP ,
   delay varchar(50) NOT NULL default '00:00:00',
   state varchar(20) NOT NULL default 'wait',

   FOREIGN KEY(type) references ms_type(typeid),
   FOREIGN KEY(source) references ms_process(procid),
   FOREIGN KEY(dest) references ms_process(procid)
   ) 
"""

        self.create['13tk_ms_subscription'] = """
CREATE TABLE ms_subscription (
   subid INTEGER PRIMARY KEY AUTOINCREMENT,
   procid int(11) NOT NULL default '0',
   typeid int(11) NOT NULL default '0',
   UNIQUE (procid,typeid),
   FOREIGN KEY(procid) references ms_process(procid),
   FOREIGN KEY(typeid) references ms_type(typeid)
   ) 
"""
        self.create['14tl_ms_subscription_priority'] = """
CREATE TABLE ms_subscription_priority (
   subid INTEGER PRIMARY KEY AUTOINCREMENT,
   procid int(11) NOT NULL default '0',
   typeid int(11) NOT NULL default '0',
   UNIQUE (procid,typeid),
   FOREIGN KEY(procid) references ms_process(procid),
   FOREIGN KEY(typeid) references ms_type(typeid)
   ) 
"""
        self.create['15tm_ms__available'] = """
CREATE TABLE ms_available (
  procid int(11) NOT NULL,
  status varchar(20) NOT NULL default 'not_there',	
  UNIQUE (procid)
  FOREIGN KEY(procid) references ms_process(procid))
"""
        
        self.create['16tn_ms_available_priority'] = """
CREATE TABLE ms_available_priority (
  procid int(11) NOT NULL,
  status varchar(20) NOT NULL default 'not_there',	
  UNIQUE (procid),
  FOREIGN KEY(procid) references ms_process(procid))
"""

        self.create['17to_ms_checkbuffer'] = """
CREATE TABLE ms_check_buffer (
  buffer varchar(100) NOT NULL,
  status varchar(20) NOT NULL default 'not_checking',
   UNIQUE (buffer)
   ) 
"""





#It's trigger time!
#SQLite doesn't appear to support the "on update CURRENT_TIMESTAMP" value for time
#Also doesn't enforce FOREIGN KEY constraints.
#Have to do triggers for all of them



        self.create["20TR_tc_ms_history"]="""
CREATE TRIGGER TR_tc_ms_history BEFORE UPDATE ON ms_history
       FOR EACH ROW
             BEGIN
                  UPDATE ms_history SET time = CURRENT_TIMESTAMP WHERE messageid = NEW.messageid;
             END;"""

        self.create['20TR_tc_ms_history_type'] = """
CREATE TRIGGER TR_ms_history_type BEFORE INSERT ON ms_history
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_message_buffer has typeid not in ms_type')
                WHERE (SELECT typeid FROM ms_type WHERE typeid = NEW.type) IS NULL;
             END;"""

        self.create['20TR_tc_ms_history_source'] = """
CREATE TRIGGER TR_ms_history_source BEFORE INSERT ON ms_history
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_history has source not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.source) IS NULL;
             END;"""

        self.create['20TR_tc_ms_history_dest'] = """
CREATE TRIGGER TR_ms_history_dest BEFORE INSERT ON ms_history
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_history has dest not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.dest) IS NULL;
             END;"""

        self.create['20TR_tc_ms_history_typeu'] = """
CREATE TRIGGER TR_ms_history_typeu BEFORE UPDATE ON ms_history
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_message_buffer has typeid not in ms_type')
                WHERE (SELECT typeid FROM ms_type WHERE typeid = NEW.type) IS NULL;
             END;"""

        self.create['20TR_tc_ms_history_sourceu'] = """
CREATE TRIGGER TR_ms_history_sourceu BEFORE UPDATE ON ms_history
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_history has source not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.source) IS NULL;
             END;"""

        self.create['20TR_tc_ms_history_destu'] = """
CREATE TRIGGER TR_ms_history_destu BEFORE UPDATE ON ms_history
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_history has dest not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.dest) IS NULL;
             END;"""

        

        #ms_history_buffer

        self.create["21TR_tca_ms_history_buffer"]="""
CREATE TRIGGER TR_tca_ms_history_buffer BEFORE UPDATE ON ms_history_buffer
       FOR EACH ROW
             BEGIN
                  UPDATE ms_history_buffer SET time = CURRENT_TIMESTAMP WHERE messageid = NEW.messageid;
             END;"""
        self.create['21TR_tca_ms_history_buffer_type'] = """
CREATE TRIGGER TR_ms_history_buffer_type BEFORE INSERT ON ms_history_buffer
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_message_buffer has typeid not in ms_type')
                WHERE (SELECT typeid FROM ms_type WHERE typeid = NEW.type) IS NULL;
             END;"""

        self.create['21TR_tca_ms_history_buffer_source'] = """
CREATE TRIGGER TR_ms_history_buffer_source BEFORE INSERT ON ms_history_buffer
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_history_buffer has source not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.source) IS NULL;
             END;"""

        self.create['21TR_tca_ms_history_buffer_dest'] = """
CREATE TRIGGER TR_ms_history_buffer_dest BEFORE INSERT ON ms_history_buffer
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_history_buffer has dest not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.dest) IS NULL;
             END;"""
        self.create['21TR_tca_ms_history_buffer_typeu'] = """
CREATE TRIGGER TR_ms_history_buffer_typeu BEFORE UPDATE ON ms_history_buffer
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_message_buffer has typeid not in ms_type')
                WHERE (SELECT typeid FROM ms_type WHERE typeid = NEW.type) IS NULL;
             END;"""

        self.create['21TR_tca_ms_history_buffer_sourceu'] = """
CREATE TRIGGER TR_ms_history_buffer_sourceu BEFORE UPDATE ON ms_history_buffer
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_history_buffer has source not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.source) IS NULL;
             END;"""

        self.create['21TR_tca_ms_history_buffer_destu'] = """
CREATE TRIGGER TR_ms_history_buffer_destu BEFORE UPDATE ON ms_history_buffer
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_history_buffer has dest not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.dest) IS NULL;
             END;"""

        

        #ms_history_priority

        self.create["22TR_td_ms_history_priority"]="""
CREATE TRIGGER TR_td_ms_history_priority BEFORE UPDATE ON ms_history_priority
       FOR EACH ROW
             BEGIN
                  UPDATE ms_history_priority SET time = CURRENT_TIMESTAMP WHERE messageid = NEW.messageid;
             END;"""

        self.create['22TR_td_ms_history_priority_type'] = """
CREATE TRIGGER TR_ms_history_priority_type BEFORE INSERT ON ms_history_priority
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_history_priority has typeid not in ms_type')
                WHERE (SELECT typeid FROM ms_type WHERE typeid = NEW.type) IS NULL;
             END;"""

        self.create['22TR_td_ms_history_priority_source'] = """
CREATE TRIGGER TR_ms_history_priority_source BEFORE INSERT ON ms_history_priority
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_history_priority has source not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.source) IS NULL;
             END;"""

        self.create['22TR_td_ms_history_priority_dest'] = """
CREATE TRIGGER TR_ms_history_priority_dest BEFORE INSERT ON ms_history_priority
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_history_priority has dest not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.dest) IS NULL;
             END;"""
        self.create['22TR_td_ms_history_priority_typeu'] = """
CREATE TRIGGER TR_ms_history_priority_typeu BEFORE UPDATE ON ms_history_priority
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_history_priority has typeid not in ms_type')
                WHERE (SELECT typeid FROM ms_type WHERE typeid = NEW.type) IS NULL;
             END;"""

        self.create['22TR_td_ms_history_priority_sourceu'] = """
CREATE TRIGGER TR_ms_history_priority_sourceu BEFORE UPDATE ON ms_history_priority
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_history_priority has source not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.source) IS NULL;
             END;"""

        self.create['22TR_td_ms_history_priority_destu'] = """
CREATE TRIGGER TR_ms_history_priority_destu BEFORE UPDATE ON ms_history_priority
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_history_priority has dest not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.dest) IS NULL;
             END;"""

        
        

        #ms_history_priority_buffer

        self.create["23TR_tda_ms_history_priority_buffer"]="""
CREATE TRIGGER TR_tda_ms_history_priority_buffer BEFORE UPDATE ON ms_history_priority_buffer
       FOR EACH ROW
             BEGIN
                  UPDATE ms_history_priority_buffer SET time = CURRENT_TIMESTAMP WHERE messageid = NEW.messageid;
             END;"""
        self.create['23TR_tda_ms_history_priority_buffer_type'] = """
CREATE TRIGGER TR_ms_history_priority_buffer_type BEFORE INSERT ON ms_history_priority_buffer
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_history_priority_buffer has typeid not in ms_type')
                WHERE (SELECT typeid FROM ms_type WHERE typeid = NEW.type) IS NULL;
             END;"""

        self.create['23TR_tda_ms_history_priority_buffer_source'] = """
CREATE TRIGGER TR_ms_history_priority_buffer_source BEFORE INSERT ON ms_history_priority_buffer
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_history_priority_buffer has source not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.source) IS NULL;
             END;"""

        self.create['23TR_tda_ms_history_priority_buffer_dest'] = """
CREATE TRIGGER TR_ms_history_priority_buffer_dest BEFORE INSERT ON ms_history_priority_buffer
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_history_priority_buffer has dest not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.dest) IS NULL;
             END;"""
        self.create['23TR_tda_ms_history_priority_buffer_typeu'] = """
CREATE TRIGGER TR_ms_history_priority_buffer_typeu BEFORE UPDATE ON ms_history_priority_buffer
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_history_priority_buffer has typeid not in ms_type')
                WHERE (SELECT typeid FROM ms_type WHERE typeid = NEW.type) IS NULL;
             END;"""

        self.create['23TR_tda_ms_history_priority_buffer_sourceu'] = """
CREATE TRIGGER TR_ms_history_priority_buffer_sourceu BEFORE UPDATE ON ms_history_priority_buffer
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_history_priority_buffer has source not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.source) IS NULL;
             END;"""

        self.create['23TR_tda_ms_history_priority_buffer_destu'] = """
CREATE TRIGGER TR_ms_history_priority_buffer_destu BEFORE UPDATE ON ms_history_priority_buffer
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_history_priority_buffer has dest not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.dest) IS NULL;
             END;"""




        #ms_message

        self.create["24TR_te_ms_message"]="""
CREATE TRIGGER TR_te_ms_message BEFORE UPDATE ON ms_message
       FOR EACH ROW
             BEGIN
                  UPDATE ms_message SET time = CURRENT_TIMESTAMP WHERE messageid = NEW.messageid;
             END;"""
        self.create['24TR_te_ms_message_type'] = """
CREATE TRIGGER TR_ms_message_type BEFORE INSERT ON ms_message
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_message has typeid not in ms_type')
                WHERE (SELECT typeid FROM ms_type WHERE typeid = NEW.type) IS NULL;
             END;"""

        self.create['24TR_te_ms_message_source'] = """
CREATE TRIGGER TR_ms_message_source BEFORE INSERT ON ms_message
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_message has source not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.source) IS NULL;
             END;"""

        self.create['24TR_te_ms_message_dest'] = """
CREATE TRIGGER TR_ms_message_dest BEFORE INSERT ON ms_message
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_message has dest not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.dest) IS NULL;
             END;"""
        self.create['24TR_te_ms_message_typeu'] = """
CREATE TRIGGER TR_ms_message_typeu BEFORE UPDATE ON ms_message
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_message has typeid not in ms_type')
                WHERE (SELECT typeid FROM ms_type WHERE typeid = NEW.type) IS NULL;
             END;"""

        self.create['24TR_te_ms_message_sourceu'] = """
CREATE TRIGGER TR_ms_message_sourceu BEFORE UPDATE ON ms_message
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_message has source not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.source) IS NULL;
             END;"""

        self.create['24TR_te_ms_message_destu'] = """
CREATE TRIGGER TR_ms_message_destu BEFORE UPDATE ON ms_message
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_message has dest not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.dest) IS NULL;
             END;"""
        




        #ms_message_buffer_in

        self.create["25TR_tf_ms_message_buffer_in"]="""
CREATE TRIGGER TR_tf_ms_message_buffer_in BEFORE UPDATE ON ms_message_buffer_in
       FOR EACH ROW
             BEGIN
                  UPDATE ms_message_buffer_in SET time = CURRENT_TIMESTAMP WHERE messageid = NEW.messageid;
             END;"""
        self.create['25TR_tf_ms_message_buffer_in_type'] = """
CREATE TRIGGER TR_ms_message_buffer_in_type BEFORE INSERT ON ms_message_buffer_in
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_message_buffer_in has typeid not in ms_type')
                WHERE (SELECT typeid FROM ms_type WHERE typeid = NEW.type) IS NULL;
             END;"""

        self.create['25TR_tf_ms_message_buffer_in_source'] = """
CREATE TRIGGER TR_ms_message_buffer_in_source BEFORE INSERT ON ms_message_buffer_in
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_message_buffer_in has source not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.source) IS NULL;
             END;"""

        self.create['25TR_tf_ms_message_buffer_in_dest'] = """
CREATE TRIGGER TR_ms_message_buffer_in_dest BEFORE INSERT ON ms_message_buffer_in
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_message_buffer_in has dest not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.dest) IS NULL;
             END;"""
        self.create['25TR_tf_ms_message_buffer_in_typeu'] = """
CREATE TRIGGER TR_ms_message_buffer_in_typeu BEFORE UPDATE ON ms_message_buffer_in
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_message_buffer_in has typeid not in ms_type')
                WHERE (SELECT typeid FROM ms_type WHERE typeid = NEW.type) IS NULL;
             END;"""

        self.create['25TR_tf_ms_message_buffer_in_sourceu'] = """
CREATE TRIGGER TR_ms_message_buffer_in_sourceu BEFORE UPDATE ON ms_message_buffer_in
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_message_buffer_in has source not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.source) IS NULL;
             END;"""

        self.create['25TR_tf_ms_message_buffer_in_destu'] = """
CREATE TRIGGER TR_ms_message_buffer_in_destu BEFORE UPDATE ON ms_message_buffer_in
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_message_buffer_in has dest not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.dest) IS NULL;
             END;"""
        



        
        #ms_message_buffer_out

        self.create["26TR_tg_ms_message_buffer_out"]="""
CREATE TRIGGER TR_tg_ms_message_buffer_out BEFORE UPDATE ON ms_message_buffer_out
       FOR EACH ROW
             BEGIN
                  UPDATE ms_message_buffer_out SET time = CURRENT_TIMESTAMP WHERE messageid = NEW.messageid;
             END;"""
        self.create['26TR_tg_ms_message_buffer_out_type'] = """
CREATE TRIGGER TR_ms_message_buffer_out_type BEFORE INSERT ON ms_message_buffer_out
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_message_buffer_out has typeid not in ms_type')
                WHERE (SELECT typeid FROM ms_type WHERE typeid = NEW.type) IS NULL;
             END;"""

        self.create['26TR_tg_ms_message_buffer_out_source'] = """
CREATE TRIGGER TR_ms_message_buffer_out_source BEFORE INSERT ON ms_message_buffer_out
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_message_buffer_out has source not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.source) IS NULL;
             END;"""

        self.create['26TR_tg_ms_message_buffer_out_dest'] = """
CREATE TRIGGER TR_ms_message_buffer_out_dest BEFORE INSERT ON ms_message_buffer_out
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_message_buffer_out has dest not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.dest) IS NULL;
             END;"""

        self.create['26TR_tg_ms_message_buffer_out_typeu'] = """
CREATE TRIGGER TR_ms_message_buffer_out_typeu BEFORE UPDATE ON ms_message_buffer_out
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_message_buffer_out has typeid not in ms_type')
                WHERE (SELECT typeid FROM ms_type WHERE typeid = NEW.type) IS NULL;
             END;"""

        self.create['26TR_tg_ms_message_buffer_out_sourceu'] = """
CREATE TRIGGER TR_ms_message_buffer_out_sourceu BEFORE UPDATE ON ms_message_buffer_out
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_message_buffer_out has source not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.source) IS NULL;
             END;"""

        self.create['26TR_tg_ms_message_buffer_out_destu'] = """
CREATE TRIGGER TR_ms_message_buffer_out_destu BEFORE UPDATE ON ms_message_buffer_out
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_message_buffer_out has dest not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.dest) IS NULL;
             END;"""




        
        #ms_priority_message

        self.create["28TR_th_ms_priority_message"]="""
CREATE TRIGGER TR_th_ms_priority_message BEFORE UPDATE ON ms_priority_message
       FOR EACH ROW
             BEGIN
                  UPDATE ms_priority_message SET time = CURRENT_TIMESTAMP WHERE messageid = NEW.messageid;
             END;"""
        self.create['28TR_th_ms_priority_message_type'] = """
CREATE TRIGGER TR_ms_priority_message_type BEFORE INSERT ON ms_priority_message
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_priority_message has typeid not in ms_type')
                WHERE (SELECT typeid FROM ms_type WHERE typeid = NEW.type) IS NULL;
             END;"""

        self.create['28TR_th_ms_priority_message_source'] = """
CREATE TRIGGER TR_ms_priority_message_source BEFORE INSERT ON ms_priority_message
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_priority_message has source not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.source) IS NULL;
             END;"""

        self.create['28TR_th_ms_priority_message_dest'] = """
CREATE TRIGGER TR_ms_priority_message_dest BEFORE INSERT ON ms_priority_message
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_priority_message has dest not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.dest) IS NULL;
             END;"""

        self.create['28TR_th_ms_priority_message_typeu'] = """
CREATE TRIGGER TR_ms_priority_message_typeu BEFORE UPDATE ON ms_priority_message
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_priority_message has typeid not in ms_type')
                WHERE (SELECT typeid FROM ms_type WHERE typeid = NEW.type) IS NULL;
             END;"""

        self.create['28TR_th_ms_priority_message_sourceu'] = """
CREATE TRIGGER TR_ms_priority_message_sourceu BEFORE UPDATE ON ms_priority_message
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_priority_message has source not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.source) IS NULL;
             END;"""

        self.create['28TR_th_ms_priority_message_destu'] = """
CREATE TRIGGER TR_ms_priority_message_destu BEFORE UPDATE ON ms_priority_message
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_priority_message has dest not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.dest) IS NULL;
             END;"""


        



        #ms_priority_message_buffer_in

        self.create["29TR_ti_ms_priority_message_buffer_in"]="""
CREATE TRIGGER TR_ti_ms_priority_message_buffer_in BEFORE UPDATE ON ms_priority_message_buffer_in
       FOR EACH ROW
             BEGIN
                  UPDATE ms_priority_message_buffer_in SET time = CURRENT_TIMESTAMP WHERE messageid = NEW.messageid;
             END;"""
        self.create['29TR_ti_ms_priority_message_buffer_in_type'] = """
CREATE TRIGGER TR_ms_priority_message_buffer_in_type BEFORE INSERT ON ms_priority_message_buffer_in
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_priority_message_buffer_in has typeid not in ms_type')
                WHERE (SELECT typeid FROM ms_type WHERE typeid = NEW.type) IS NULL;
             END;"""

        self.create['29TR_ti_ms_priority_message_buffer_in_source'] = """
CREATE TRIGGER TR_ms_priority_message_buffer_in_source BEFORE INSERT ON ms_priority_message_buffer_in
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_priority_message_buffer_in has source not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.source) IS NULL;
             END;"""

        self.create['29TR_ti_ms_priority_message_buffer_in_dest'] = """
CREATE TRIGGER TR_ms_priority_message_buffer_in_dest BEFORE INSERT ON ms_priority_message_buffer_in
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_priority_message_buffer_in has dest not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.dest) IS NULL;
             END;"""

        self.create['29TR_ti_ms_priority_message_buffer_in_typeu'] = """
CREATE TRIGGER TR_ms_priority_message_buffer_in_typeu BEFORE UPDATE ON ms_priority_message_buffer_in
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_priority_message_buffer_in has typeid not in ms_type')
                WHERE (SELECT typeid FROM ms_type WHERE typeid = NEW.type) IS NULL;
             END;"""

        self.create['29TR_ti_ms_priority_message_buffer_in_sourceu'] = """
CREATE TRIGGER TR_ms_priority_message_buffer_in_sourceu BEFORE UPDATE ON ms_priority_message_buffer_in
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_priority_message_buffer_in has source not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.source) IS NULL;
             END;"""

        self.create['29TR_ti_ms_priority_message_buffer_in_destu'] = """
CREATE TRIGGER TR_ms_priority_message_buffer_in_destu BEFORE UPDATE ON ms_priority_message_buffer_in
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_priority_message_buffer_in has dest not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.dest) IS NULL;
             END;"""

        

        
        #ms_priority_message_buffer_out

        self.create["30TR_tj_ms_priority_message_buffer_out"]="""
CREATE TRIGGER TR_tj_ms_priority_message_buffer_out BEFORE UPDATE ON ms_priority_message_buffer_out
       FOR EACH ROW
             BEGIN
                  UPDATE ms_priority_message_buffer_out SET time = CURRENT_TIMESTAMP WHERE messageid = NEW.messageid;
             END;"""
        self.create['30TR_tj_ms_priority_message_buffer_out_type'] = """
CREATE TRIGGER TR_ms_priority_message_buffer_out_type BEFORE INSERT ON ms_priority_message_buffer_out
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_priority_message_buffer_out has typeid not in ms_type')
                WHERE (SELECT typeid FROM ms_type WHERE typeid = NEW.type) IS NULL;
             END;"""

        self.create['30TR_tj_ms_priority_message_buffer_out_source'] = """
CREATE TRIGGER TR_ms_priority_message_buffer_out_source BEFORE INSERT ON ms_priority_message_buffer_out
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_priority_message_buffer_out has source not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.source) IS NULL;
             END;"""

        self.create['30TR_tj_ms_priority_message_buffer_out_dest'] = """
CREATE TRIGGER TR_ms_priority_message_buffer_out_dest BEFORE INSERT ON ms_priority_message_buffer_out
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_priority_message_buffer_out has dest not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.dest) IS NULL;
             END;"""

        self.create['30TR_tj_ms_priority_message_buffer_out_typeu'] = """
CREATE TRIGGER TR_ms_priority_message_buffer_out_typeu BEFORE UPDATE ON ms_priority_message_buffer_out
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_priority_message_buffer_out has typeid not in ms_type')
                WHERE (SELECT typeid FROM ms_type WHERE typeid = NEW.type) IS NULL;
             END;"""

        self.create['30TR_tj_ms_priority_message_buffer_out_sourceu'] = """
CREATE TRIGGER TR_ms_priority_message_buffer_out_sourceu BEFORE UPDATE ON ms_priority_message_buffer_out
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_priority_message_buffer_out has source not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.source) IS NULL;
             END;"""

        self.create['30TR_tj_ms_priority_message_buffer_out_destu'] = """
CREATE TRIGGER TR_ms_priority_message_buffer_out_destu BEFORE UPDATE ON ms_priority_message_buffer_out
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_priority_message_buffer_out has dest not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.dest) IS NULL;
             END;"""

        #ms_subscription

        self.create['31TR_tk_ms_subscription_typeid'] = """
CREATE TRIGGER TR_ms_subscription_type BEFORE INSERT ON ms_subscription
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_subscription has typeid not in ms_type')
                WHERE (SELECT typeid FROM ms_type WHERE typeid = NEW.typeid) IS NULL;
             END;"""

        self.create['31TR_tk_ms_subscription_procid'] = """
CREATE TRIGGER TR_ms_subscription_source BEFORE INSERT ON ms_subscription
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_subscription has procid not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.procid) IS NULL;
             END;"""

        self.create['31TR_tk_ms_subscription_typeidu'] = """
CREATE TRIGGER TR_ms_subscription_typeu BEFORE UPDATE ON ms_subscription
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_subscription has typeid not in ms_type')
                WHERE (SELECT typeid FROM ms_type WHERE typeid = NEW.typeid) IS NULL;
             END;"""

        self.create['31TR_tk_ms_subscription_procidu'] = """
CREATE TRIGGER TR_ms_subscription_sourceu BEFORE UPDATE ON ms_subscription
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_subscription has procid not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.procid) IS NULL;
             END;"""



        #ms_subscription_priority


        self.create['32TR_tl_ms_subscription_priority_typeid'] = """
CREATE TRIGGER TR_ms_subscription_priority_type BEFORE INSERT ON ms_subscription_priority
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_subscription_priority has typeid not in ms_type')
                WHERE (SELECT typeid FROM ms_type WHERE typeid = NEW.typeid) IS NULL;
             END;"""

        self.create['32TR_tl_ms_subscription_priority_procid'] = """
CREATE TRIGGER TR_ms_subscription_priority_procid BEFORE INSERT ON ms_subscription_priority
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_subscription_priority has procid not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.procid) IS NULL;
             END;"""

        self.create['32TR_tl_ms_subscription_priority_typeidu'] = """
CREATE TRIGGER TR_ms_subscription_priority_typeu BEFORE UPDATE ON ms_subscription_priority
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_subscription_priority has typeid not in ms_type')
                WHERE (SELECT typeid FROM ms_type WHERE typeid = NEW.typeid) IS NULL;
             END;"""

        self.create['32TR_tl_ms_subscription_priority_procidu'] = """
CREATE TRIGGER TR_ms_subscription_priority_procidu BEFORE UPDATE ON ms_subscription_priority
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_subscription_priority has procid not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.procid) IS NULL;
             END;"""



        


        self.create['33TR_tm_ms_available_procid'] = """
CREATE TRIGGER TR_ms_available_source BEFORE INSERT ON ms_available
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_available has procid not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.procid) IS NULL;
             END;"""

        self.create['33TR_tm_ms_available_procidu'] = """
CREATE TRIGGER TR_ms_available_sourceu BEFORE UPDATE ON ms_available
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_available has procid not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.procid) IS NULL;
             END;"""



        

        #ms_available_priority_status

        self.create['34TR_tn_ms_available_priority_procid'] = """
CREATE TRIGGER TR_ms_available_priority_source BEFORE INSERT ON ms_available_priority
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_available_priority has procid not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.procid) IS NULL;
             END;"""

        self.create['34TR_tn_ms_available_priority_procidu'] = """
CREATE TRIGGER TR_ms_available_priority_sourceu BEFORE UPDATE ON ms_available_priority
       FOR EACH ROW
             BEGIN
                SELECT RAISE(ROLLBACK, 'insert on table ms_available_priority has procid not in ms_process')
                WHERE (SELECT procid FROM ms_process WHERE procid = NEW.procid) IS NULL;
             END;"""
