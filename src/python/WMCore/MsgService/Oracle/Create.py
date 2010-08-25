#!/usr/bin/python
#pylint: disable-msg=E1103

"""
_Create_

Class for creating Oracle specific schema for persistent messages.

"""

__revision__ = "$Id: Create.py,v 1.5 2009/08/12 21:07:38 sryu Exp $"
__version__ = "$Revision: 1.5 $"
__author__ = "fvlingen@caltech.edu"

import logging
import threading

from WMCore.Database.DBCreator import DBCreator

class Create(DBCreator):
    """
    _Create_
    
    Class for creating MySQL specific schema for persistent messages.
    """

    sequence_tables = []

    sequence_tables.append('ms_process_seq')
    sequence_tables.append('ms_type_seq')
    sequence_tables.append('ms_history_seq')
    sequence_tables.append('ms_history_buffer_seq')
    sequence_tables.append('ms_history_priority_seq')
    sequence_tables.append('ms_history_priority_buff_seq')
    sequence_tables.append('ms_message_seq')
    sequence_tables.append('ms_message_buffer_in_seq')
    sequence_tables.append('ms_message_buffer_out_seq')
    sequence_tables.append('ms_priority_message_seq')
    sequence_tables.append('ms_prio_message_buff_in_seq')
    sequence_tables.append('ms_prio_msg_buff_out_seq')
    sequence_tables.append('ms_subscription_seq')
    sequence_tables.append('ms_subscription_prio_seq')

    
    
    def __init__(self,logger=None, dbi=None):
        myThread = threading.currentThread()

        if logger == None:
            logger = myThread.logger
        if dbi == None:
            dbi = myThread.dbi
            
        DBCreator.__init__(self, logger, dbi)
        self.create = {}
        self.constraints = {}

        #ms_process
        
        self.create['a_ms_process'] = """
CREATE TABLE ms_process
(       procid     NUMBER            NOT NULL ENABLE,
        name       VARCHAR2(40 BYTE) NOT NULL ENABLE,
        host       VARCHAR2(60 BYTE) NOT NULL ENABLE,
        pid        NUMBER            NOT NULL ENABLE,
        CONSTRAINT ms_process_pk     PRIMARY KEY (procid),
        CONSTRAINT ms_process_unique UNIQUE (name, pid)
)
"""
        self.create['a_ms_process_seq'] = """
CREATE SEQUENCE ms_process_seq
        start with 1
        increment by 1
        nomaxvalue
"""
        self.create['a_ms_process_trig'] = """
CREATE TRIGGER ms_process_trig
BEFORE INSERT ON ms_process
REFERENCING NEW AS NEW
FOR EACH ROW
    BEGIN
         SELECT ms_process_seq.nextval INTO :NEW.procid FROM dual;
    END;"""

        #ms_type
        
        self.create['d_ms_type'] = """
CREATE TABLE ms_type
(       typeid  NUMBER NOT NULL ENABLE,
        name  VARCHAR2(255 BYTE) NOT NULL ENABLE,
        CONSTRAINT "MS_TYPE_PK" PRIMARY KEY (typeid),
        CONSTRAINT "MS_TYPE_UK1" UNIQUE (name)
)
        """
        self.create['d_ms_type_seq'] = """
CREATE SEQUENCE ms_type_seq
start with 1
increment by 1
nomaxvalue
        """
        self.create['d_ms_type_tr'] = """
CREATE TRIGGER ms_type_tr
BEFORE INSERT ON ms_type
REFERENCING NEW AS NEW
FOR EACH ROW
    DECLARE m_no INTEGER;
    BEGIN
         SELECT ms_type_seq.nextval INTO :new.typeid FROM dual;
    END;        """


        

        #ms_history
#            time      timestamp      default CURRENT_TIMESTAMP
#    on update CURRENT_TIMESTAMP,

        self.create['e_ms_history'] = """
CREATE TABLE ms_history (
    messageid NUMBER(11)                        NOT NULL ENABLE,
    type      NUMBER(11)     default '0'        NOT NULL ENABLE,
    source    NUMBER(11)     default '0'        NOT NULL ENABLE,
    dest      NUMBER(11)     default '0'        NOT NULL ENABLE,
    payload   CLOB                              NOT NULL ENABLE,
    delay     varchar2(50)   default '00:00:00' NOT NULL ENABLE,
    time      timestamp      default CURRENT_TIMESTAMP    NOT NULL ENABLE,

    CONSTRAINT e_ms_history_type   FOREIGN KEY (type)   REFERENCES ms_type(typeid),
    CONSTRAINT e_ms_history_source FOREIGN KEY (source) REFERENCES ms_process(procid),
    CONSTRAINT e_ms_history_dest   FOREIGN KEY (dest)   REFERENCES ms_process(procid),
    CONSTRAINT e_ms_history_pk     PRIMARY KEY (messageid)
    )
"""


        self.create['e_ms_history_seq'] = """
CREATE SEQUENCE ms_history_seq
start with 1
increment by 1
nomaxvalue
        """

        self.create['e_ms_history_tr'] = """
CREATE TRIGGER ms_history_tr
BEFORE INSERT ON ms_history
REFERENCING NEW AS NEW
FOR EACH ROW
    DECLARE m_no INTEGER;
    BEGIN
         SELECT ms_history_seq.nextval INTO :new.messageid FROM dual;
    END;        """

        self.create['e_ms_history_timetrig'] = """
CREATE TRIGGER ms_history_timetrig BEFORE UPDATE ON ms_history
REFERENCING NEW AS NEW
FOR EACH ROW
     BEGIN
          SET NEW.time = CURRENT_TIMESTAMP;
     END;        """



        #ms_history_buffer

        self.create['f_ms_history_buffer'] = """
CREATE TABLE ms_history_buffer (
    messageid  NUMBER(11)                 NOT NULL ENABLE,
    type       NUMBER(11)   default '0'   NOT NULL ENABLE,
    source     NUMBER(11)   default '0'   NOT NULL ENABLE,
    dest       NUMBER(11)   default '0'   NOT NULL ENABLE,
    payload    CLOB                       NOT NULL ENABLE,
    delay      varchar2(50) default '00:00:00'            NOT NULL ENABLE,
    time       timestamp    default CURRENT_TIMESTAMP     NOT NULL ENABLE,

    CONSTRAINT ms_history_buffer_pk     PRIMARY KEY (messageid),
    CONSTRAINT ms_history_buffer_type   FOREIGN KEY (type)   REFERENCES ms_type(typeid),
    CONSTRAINT ms_history_buffer_dest   FOREIGN KEY (dest)   REFERENCES ms_process(procid),
    CONSTRAINT ms_history_buffer_source FOREIGN KEY (source) REFERENCES ms_process(procid)
    )"""

        self.create['f_ms_history_buffer_seq'] = """
CREATE SEQUENCE ms_history_buffer_seq
start with 1
increment by 1
nomaxvalue
        """


        self.create['f_ms_history_buffer_tr'] = """
CREATE TRIGGER ms_history_buffer_tr
BEFORE INSERT ON ms_history_buffer
REFERENCING NEW AS NEW
FOR EACH ROW
    DECLARE m_no INTEGER;
    BEGIN
         SELECT ms_history_buffer_seq.nextval INTO :new.messageid FROM dual;
    END;        """

        self.create['f_ms_history_buffer_timetrig'] = """
CREATE TRIGGER ms_history_buffer_timetrig BEFORE UPDATE ON ms_history_buffer
REFERENCING NEW AS NEW
FOR EACH ROW
     BEGIN
          SET NEW.time = CURRENT_TIMESTAMP;
     END;        """


        #ms_history_priority


        self.create['g_ms_history_priority'] = """
CREATE TABLE ms_history_priority (
   messageid NUMBER(11)                NOT NULL ENABLE,
   type      NUMBER(11)   default '0'  NOT NULL ENABLE,
   source    NUMBER(11)   default '0'  NOT NULL ENABLE,
   dest      NUMBER(11)   default '0'  NOT NULL ENABLE,
   payload   CLOB                      NOT NULL ENABLE,
   time      timestamp    default CURRENT_TIMESTAMP   NOT NULL ENABLE,
   delay     varchar2(50) default '00:00:00'          NOT NULL ENABLE,

   CONSTRAINT ms_history_priority_pk     PRIMARY KEY (messageid),
   CONSTRAINT ms_history_priority_type   FOREIGN KEY (type)   REFERENCES ms_type(typeid),
   CONSTRAINT ms_history_priority_dest   FOREIGN KEY (dest)   REFERENCES ms_process(procid),
   CONSTRAINT ms_history_priority_source FOREIGN KEY (source) REFERENCES ms_process(procid)
    )"""

        self.create['g_ms_history_priority_seq'] = """
CREATE SEQUENCE ms_history_priority_seq
start with 1
increment by 1
nomaxvalue
        """
        
        self.create['g_ms_history_priority_tr'] = """
CREATE TRIGGER ms_history_priority_tr
BEFORE INSERT ON ms_history_priority
REFERENCING NEW AS NEW
FOR EACH ROW
    DECLARE m_no INTEGER;
    BEGIN
         SELECT ms_history_priority_seq.nextval INTO :new.messageid FROM dual;
    END;        """

        self.create['g_ms_history_priority_timetrig'] = """
CREATE TRIGGER ms_history_priority_timetrig BEFORE UPDATE ON ms_history_priority
REFERENCING NEW AS NEW
FOR EACH ROW
     BEGIN
          SET NEW.time = CURRENT_TIMESTAMP;
     END;        """




        #ms_history_priority_buffer


        self.create['h_ms_history_priority_buffer'] = """
CREATE TABLE ms_history_priority_buffer (
   messageid NUMBER(11)                NOT NULL ENABLE,
   type      NUMBER(11)   default '0'  NOT NULL ENABLE,
   source    NUMBER(11)   default '0'  NOT NULL ENABLE,
   dest      NUMBER(11)   default '0'  NOT NULL ENABLE,
   payload   CLOB                      NOT NULL ENABLE,
   time      timestamp    default CURRENT_TIMESTAMP   NOT NULL ENABLE,
   delay     varchar2(50) default '00:00:00'          NOT NULL ENABLE,

   CONSTRAINT ms_history_priority_buff_pk     PRIMARY KEY (messageid),
   CONSTRAINT ms_history_priority_buff_type   FOREIGN KEY (type)   REFERENCES ms_type(typeid),
   CONSTRAINT ms_history_priority_buff_dest   FOREIGN KEY (dest)   REFERENCES ms_process(procid),
   CONSTRAINT ms_history_priority_buff_src    FOREIGN KEY (source) REFERENCES ms_process(procid)
    )"""

        self.create['h_ms_history_priority_buffer_seq'] = """
CREATE SEQUENCE ms_history_priority_buff_seq
start with 1
increment by 1
nomaxvalue
        """

        self.create['h_ms_history_priority_buffer_tr'] = """
CREATE TRIGGER ms_history_priority_buff_tr
BEFORE INSERT ON ms_history_priority_buffer
REFERENCING NEW AS NEW
FOR EACH ROW
    DECLARE m_no INTEGER;
    BEGIN
         SELECT ms_history_priority_buff_seq.nextval INTO :new.messageid FROM dual;
    END;        """

        self.create['h_ms_history_priority_buffer_timetrig'] = """
CREATE TRIGGER ms_history_prio_buff_timetrig BEFORE UPDATE ON ms_history_priority_buffer
REFERENCING NEW AS NEW
FOR EACH ROW
     BEGIN
          SET NEW.time = CURRENT_TIMESTAMP;
     END;        """



        #ms_message

        self.create['i_ms_message'] = """
CREATE TABLE ms_message (
   messageid NUMBER(11)                NOT NULL ENABLE,
   type      NUMBER(11)   default '0'  NOT NULL ENABLE,
   source    NUMBER(11)   default '0'  NOT NULL ENABLE,
   dest      NUMBER(11)   default '0'  NOT NULL ENABLE,
   payload   CLOB                      NOT NULL ENABLE,
   time      timestamp    default CURRENT_TIMESTAMP   NOT NULL ENABLE,
   delay     varchar2(50) default '00:00:00'          NOT NULL ENABLE,

   CONSTRAINT ms_message_pk     PRIMARY KEY (messageid),
   CONSTRAINT ms_message_type   FOREIGN KEY (type)   REFERENCES ms_type(typeid),
   CONSTRAINT ms_message_dest   FOREIGN KEY (dest)   REFERENCES ms_process(procid),
   CONSTRAINT ms_message_source FOREIGN KEY (source) REFERENCES ms_process(procid)
    )"""

        self.create['i_ms_message_seq'] = """
CREATE SEQUENCE ms_message_seq
start with 1
increment by 1
nomaxvalue
        """

        self.create['i_ms_message_tr'] = """
CREATE TRIGGER ms_message_tr
BEFORE INSERT ON ms_message
REFERENCING NEW AS NEW
FOR EACH ROW
    DECLARE m_no INTEGER;
    BEGIN
         SELECT ms_message_seq.nextval INTO :new.messageid FROM dual;
    END;        """

        self.create['i_ms_message_timetrig'] = """
CREATE TRIGGER ms_message_timetrig BEFORE UPDATE ON ms_message
REFERENCING NEW AS NEW
FOR EACH ROW
     BEGIN
          SET NEW.time = CURRENT_TIMESTAMP;
     END;        """



        #ms_message_buffer_in

        self.create['j_ms_message_buffer_in'] = """
CREATE TABLE ms_message_buffer_in (
   messageid NUMBER(11)                NOT NULL ENABLE,
   type      NUMBER(11)   default '0'  NOT NULL ENABLE,
   source    NUMBER(11)   default '0'  NOT NULL ENABLE,
   dest      NUMBER(11)   default '0'  NOT NULL ENABLE,
   payload   CLOB                      NOT NULL ENABLE,
   time      timestamp    default CURRENT_TIMESTAMP   NOT NULL ENABLE,
   delay     varchar2(50) default '00:00:00'          NOT NULL ENABLE,

   CONSTRAINT ms_message_buffer_in_pk     PRIMARY KEY (messageid),
   CONSTRAINT ms_message_buffer_in_type   FOREIGN KEY (type)   REFERENCES ms_type(typeid),
   CONSTRAINT ms_message_buffer_in_dest   FOREIGN KEY (dest)   REFERENCES ms_process(procid),
   CONSTRAINT ms_message_buffer_in_source FOREIGN KEY (source) REFERENCES ms_process(procid)
    )"""

        self.create['j_ms_message_buffer_in_seq'] = """
CREATE SEQUENCE ms_message_buffer_in_seq
start with 1
increment by 1
nomaxvalue
        """

        self.create['j_ms_message_buffer_in_tr'] = """
CREATE TRIGGER ms_message_buffer_in_tr
BEFORE INSERT ON ms_message_buffer_in
REFERENCING NEW AS NEW
FOR EACH ROW
    DECLARE m_no INTEGER;
    BEGIN
         SELECT ms_message_buffer_in_seq.nextval INTO :new.messageid FROM dual;
    END;        """

        self.create['j_ms_message_buffer_in_timetrig'] = """
CREATE TRIGGER ms_message_buffer_in_timetrig BEFORE UPDATE ON ms_message_buffer_in
REFERENCING NEW AS NEW
FOR EACH ROW
     BEGIN
          SET NEW.time = CURRENT_TIMESTAMP;
     END;        """



        #ms_message_buffer_out

        self.create['k_ms_message_buffer_out'] = """
CREATE TABLE ms_message_buffer_out (
   messageid NUMBER(11)                NOT NULL ENABLE,
   type      NUMBER(11)   default '0'  NOT NULL ENABLE,
   source    NUMBER(11)   default '0'  NOT NULL ENABLE,
   dest      NUMBER(11)   default '0'  NOT NULL ENABLE,
   payload   CLOB                      NOT NULL ENABLE,
   time      timestamp    default CURRENT_TIMESTAMP   NOT NULL ENABLE,
   delay     varchar2(50) default '00:00:00'          NOT NULL ENABLE,
   state     VARCHAR2(20) default 'wait',

   CONSTRAINT ms_message_buffer_out_pk     PRIMARY KEY (messageid),
   CONSTRAINT ms_message_buffer_out_type   FOREIGN KEY (type)   REFERENCES ms_type(typeid),
   CONSTRAINT ms_message_buffer_out_dest   FOREIGN KEY (dest)   REFERENCES ms_process(procid),
   CONSTRAINT ms_message_buffer_out_source FOREIGN KEY (source) REFERENCES ms_process(procid)
    )"""

        self.create['k_ms_message_buffer_out_seq'] = """
CREATE SEQUENCE ms_message_buffer_out_seq
start with 1
increment by 1
nomaxvalue
        """

        self.create['k_ms_message_buffer_out_tr'] = """
CREATE TRIGGER ms_message_buffer_out_tr
BEFORE INSERT ON ms_message_buffer_out
REFERENCING NEW AS NEW
FOR EACH ROW
    DECLARE m_no INTEGER;
    BEGIN
         SELECT ms_message_buffer_out_seq.nextval INTO :new.messageid FROM dual;
    END;        """

        self.create['k_ms_message_buffer_out_timetrig'] = """
CREATE TRIGGER ms_message_buffer_out_timetrig BEFORE UPDATE ON ms_message_buffer_out
REFERENCING NEW AS NEW
FOR EACH ROW
     BEGIN
          :NEW.time := CURRENT_TIMESTAMP;
     END;        """



        #ms_priority_message

        self.create['l_ms_priority_message'] = """
CREATE TABLE ms_priority_message (
   messageid NUMBER(11)                NOT NULL ENABLE,
   type      NUMBER(11)   default '0'  NOT NULL ENABLE,
   source    NUMBER(11)   default '0'  NOT NULL ENABLE,
   dest      NUMBER(11)   default '0'  NOT NULL ENABLE,
   payload   CLOB                      NOT NULL ENABLE,
   time      timestamp    default CURRENT_TIMESTAMP   NOT NULL ENABLE,
   delay     varchar2(50) default '00:00:00'          NOT NULL ENABLE,

   CONSTRAINT ms_priority_message_pk     PRIMARY KEY (messageid),
   CONSTRAINT ms_priority_message_type   FOREIGN KEY (type)   REFERENCES ms_type(typeid),
   CONSTRAINT ms_priority_message_dest   FOREIGN KEY (dest)   REFERENCES ms_process(procid),
   CONSTRAINT ms_priority_message_source FOREIGN KEY (source) REFERENCES ms_process(procid)
    )"""

        self.create['l_ms_priority_message_seq'] = """
CREATE SEQUENCE ms_priority_message_seq
start with 1
increment by 1
nomaxvalue
        """

        self.create['l_ms_priority_message_tr'] = """
CREATE TRIGGER ms_priority_message_tr
BEFORE INSERT ON ms_priority_message
REFERENCING NEW AS NEW
FOR EACH ROW
    DECLARE m_no INTEGER;
    BEGIN
         SELECT ms_priority_message_seq.nextval INTO :new.messageid FROM dual;
    END;        """

        self.create['l_ms_priority_message_timetrig'] = """
CREATE TRIGGER ms_priority_message_timetrig BEFORE UPDATE ON ms_priority_message
REFERENCING NEW AS NEW
FOR EACH ROW
     BEGIN
          SET NEW.time = CURRENT_TIMESTAMP;
     END;        """






        #ms_priority_message_buffer_in

        self.create['m_ms_priority_message_buffer_in'] = """
CREATE TABLE ms_priority_message_buffer_in (
   messageid NUMBER(11)                NOT NULL ENABLE,
   type      NUMBER(11)   default '0'  NOT NULL ENABLE,
   source    NUMBER(11)   default '0'  NOT NULL ENABLE,
   dest      NUMBER(11)   default '0'  NOT NULL ENABLE,
   payload   CLOB                      NOT NULL ENABLE,
   time      timestamp    default CURRENT_TIMESTAMP   NOT NULL ENABLE,
   delay     varchar2(50) default '00:00:00'          NOT NULL ENABLE,

   CONSTRAINT ms_prio_message_buff_in_pk     PRIMARY KEY (messageid),
   CONSTRAINT ms_prio_message_buff_in_type   FOREIGN KEY (type)   REFERENCES ms_type(typeid),
   CONSTRAINT ms_prio_message_buff_in_dest   FOREIGN KEY (dest)   REFERENCES ms_process(procid),
   CONSTRAINT ms_prio_message_buff_in_source FOREIGN KEY (source) REFERENCES ms_process(procid)
    )"""

        self.create['m_ms_priority_message_buffer_in_seq'] = """
CREATE SEQUENCE ms_prio_message_buff_in_seq
start with 1
increment by 1
nomaxvalue
        """

        self.create['m_ms_priority_message_buffer_in_tr'] = """
CREATE TRIGGER ms_prio_message_buff_in_tr
BEFORE INSERT ON ms_priority_message_buffer_in
REFERENCING NEW AS NEW
FOR EACH ROW
    DECLARE m_no INTEGER;
    BEGIN
         SELECT ms_prio_message_buff_in_seq.nextval INTO :new.messageid FROM dual;
    END;        """

        self.create['m_ms_priority_message_buffer_in_timetrig'] = """
CREATE TRIGGER ms_prio_msg_buff_in_timetrig BEFORE UPDATE ON ms_priority_message_buffer_in
REFERENCING NEW AS NEW
FOR EACH ROW
     BEGIN
          SET NEW.time = CURRENT_TIMESTAMP;
     END;        """






        #ms_priority_message_buffer_out

        self.create['n_ms_priority_message_buffer_out'] = """
CREATE TABLE ms_priority_message_buffer_out (
   messageid NUMBER(11)                NOT NULL ENABLE,
   type      NUMBER(11)   default '0'  NOT NULL ENABLE,
   source    NUMBER(11)   default '0'  NOT NULL ENABLE,
   dest      NUMBER(11)   default '0'  NOT NULL ENABLE,
   payload   CLOB                      NOT NULL ENABLE,
   time      timestamp    default CURRENT_TIMESTAMP   NOT NULL ENABLE,
   delay     varchar2(50) default '00:00:00'          NOT NULL ENABLE,
   state     varchar2(20) default 'wait',

   CONSTRAINT ms_prio_msg_buff_out_state  CHECK(state IN ('queued', 'processing', 'wait')),
   CONSTRAINT ms_prio_msg_buff_out_pk     PRIMARY KEY (messageid),
   CONSTRAINT ms_prio_msg_buff_out_type   FOREIGN KEY (type)   REFERENCES ms_type(typeid),
   CONSTRAINT ms_prio_msg_buff_out_dest   FOREIGN KEY (dest)   REFERENCES ms_process(procid),
   CONSTRAINT ms_prio_msg_buff_out_source FOREIGN KEY (source) REFERENCES ms_process(procid)
    )"""

        self.create['n_ms_priority_message_buffer_out_seq'] = """
CREATE SEQUENCE ms_prio_msg_buff_out_seq
start with 1
increment by 1
nomaxvalue
        """

        self.create['n_ms_priority_message_buffer_out_tr'] = """
CREATE TRIGGER ms_prio_msg_buff_out_tr
BEFORE INSERT ON ms_priority_message_buffer_out
REFERENCING NEW AS NEW
FOR EACH ROW
    DECLARE m_no INTEGER;
    BEGIN
         SELECT ms_prio_msg_buff_out_seq.nextval INTO :new.messageid FROM dual;
    END;        """

        self.create['n_ms_priority_message_buffer_out_timetrig'] = """
CREATE TRIGGER ms_prio_msg_buff_out_timetrig BEFORE UPDATE ON ms_priority_message_buffer_out
REFERENCING NEW AS NEW
FOR EACH ROW
     BEGIN
          :NEW.time := CURRENT_TIMESTAMP;
     END;        """


        #ms_subscription

        self.create['o_ms_subscription'] = """
CREATE TABLE ms_subscription (
   subid  NUMBER(11)              NOT NULL ENABLE,
   procid NUMBER(11) default '0'  NOT NULL ENABLE,
   typeid NUMBER(11) default '0'  NOT NULL ENABLE,

   CONSTRAINT ms_subscription_pk      PRIMARY KEY (subid),
   CONSTRAINT ms_subscription_unique  UNIQUE(procid, typeid),
   CONSTRAINT ms_subscription_procid  FOREIGN KEY (procid) REFERENCES ms_process(procid),
   CONSTRAINT ms_subscription_typeid  FOREIGN KEY (typeid) REFERENCES ms_type   (typeid)
   )"""


        self.create['o_ms_subscription_seq'] = """
CREATE SEQUENCE ms_subscription_seq
start with 1
increment by 1
nomaxvalue
        """

        self.create['o_ms_subscription_tr'] = """
CREATE TRIGGER ms_subscription_tr
BEFORE INSERT ON ms_subscription
REFERENCING NEW AS NEW
FOR EACH ROW
    DECLARE m_no INTEGER;
    BEGIN
         SELECT ms_subscription_seq.nextval INTO :new.subid FROM dual;
    END;        """



        #ms_subscription_priority

        self.create['p_ms_subscription_priority'] = """
CREATE TABLE ms_subscription_priority (
   subid  NUMBER(11)              NOT NULL ENABLE,
   procid NUMBER(11) default '0'  NOT NULL ENABLE,
   typeid NUMBER(11) default '0'  NOT NULL ENABLE,

   CONSTRAINT ms_subscription_prio_pk     PRIMARY KEY (subid),
   CONSTRAINT ms_subscription_prio_unique UNIQUE(procid, typeid),
   CONSTRAINT ms_subscription_prio_procid FOREIGN KEY (procid) REFERENCES ms_process(procid),
   CONSTRAINT ms_subscription_prio_typeid FOREIGN KEY (typeid) REFERENCES ms_type   (typeid)
   )"""



        self.create['p_ms_subscription_priority_seq'] = """
CREATE SEQUENCE ms_subscription_prio_seq
start with 1
increment by 1
nomaxvalue
        """

        self.create['p_ms_subscription_priority_tr'] = """
CREATE TRIGGER ms_subscription_prio_tr
BEFORE INSERT ON ms_subscription_priority
REFERENCING NEW AS NEW
FOR EACH ROW
    DECLARE m_no INTEGER;
    BEGIN
         SELECT ms_subscription_prio_seq.nextval INTO :new.subid FROM dual;
    END;        """



        #ms_available

        self.create['q_ms_available'] = """
CREATE TABLE ms_available (
  procid   NUMBER(11)    NOT NULL ENABLE,
  status   VARCHAR2(20)  default 'not_there',

  CONSTRAINT ms_available_unique   UNIQUE(procid),
  CONSTRAINT ms_available_status   CHECK(status IN ('there', 'not_there')),
  CONSTRAINT ms_available_procid   FOREIGN KEY (procid) REFERENCES ms_process(procid)
  ) """



        #ms_available_priority
        
        self.create['r_ms_available_priority'] = """
CREATE TABLE ms_available_priority (
  procid NUMBER(11)  NOT NULL ENABLE,
  status   VARCHAR2(20)  default 'not_there',

  CONSTRAINT ms_available_priority_unique   UNIQUE(procid),
  CONSTRAINT ms_available_priority_status   CHECK(status IN ('there', 'not_there')),
  CONSTRAINT ms_available_priority_procid   FOREIGN KEY (procid) REFERENCES ms_process(procid)
   )"""



        #ms_checkbuffer

        self.create['s_ms_check_buffer'] = """
CREATE TABLE ms_check_buffer (
   buffer   varchar2(100)  NOT NULL ENABLE,
   status   VARCHAR2(20)   default 'not_checking',

   CONSTRAINT ms_check_buffer_status CHECK(status IN ('checking', 'not_checking')),
   CONSTRAINT ms_check_buffer_unique UNIQUE(buffer)
  )
"""

