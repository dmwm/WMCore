#!/usr/bin/python
"""
_Create_

Class for creating Oracle specific schema for persistent messages.
"""

__revision__ = "$Id: Create.py,v 1.6 2009/09/02 15:05:45 sfoulkes Exp $"
__version__ = "$Revision: 1.6 $"

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

    def __init__(self, logger = None, dbi = None, params = None):
        myThread = threading.currentThread()

        if logger == None:
            logger = myThread.logger
        if dbi == None:
            dbi = myThread.dbi
            
        DBCreator.__init__(self, logger, dbi)

        tablespaceTable = ""
        tablespaceIndex = ""

        if params:
            if params.has_key("tablespace_table"):
                tablespaceTable = "TABLESPACE %s" % params["tablespace_table"]
            if params.has_key("tablespace_index"):
                tablespaceIndex = "USING INDEX TABLESPACE %s" % params["tablespace_index"]
                                                            
        
        self.create["a_ms_process"] = \
          """CREATE TABLE ms_process (
               procid     NUMBER            NOT NULL ENABLE,
               name       VARCHAR2(40 BYTE) NOT NULL ENABLE,
               host       VARCHAR2(60 BYTE) NOT NULL ENABLE,
               pid        NUMBER            NOT NULL ENABLE
               ) %s""" % tablespaceTable

        self.create["a_ms_process_seq"] = \
          """CREATE SEQUENCE ms_process_seq
               START WITH 1
               INCREMENT BY 1
               NOMAXVALUE"""

        self.create["a_ms_process_trg"] = \
          """CREATE TRIGGER ms_process_trig
               BEFORE INSERT ON ms_process
               REFERENCING NEW AS NEW
               FOR EACH ROW
                 BEGIN
                   SELECT ms_process_seq.nextval INTO :NEW.procid FROM dual;
                 END;"""

        self.indexes["a_pk_ms_process"] = \
          """ALTER TABLE ms_process ADD                              
               (CONSTRAINT ms_process_pk PRIMARY KEY (procid) %s)""" % tablespaceIndex

        self.indexes["a_uk_ms_process"] = \
          """ALTER TABLE ms_process ADD                              
               (CONSTRAINT ms_process_unique UNIQUE (name, pid) %s)""" % tablespaceIndex

        self.create["d_ms_type"] = \
          """CREATE TABLE ms_type (
               typeid NUMBER             NOT NULL ENABLE,
               name   VARCHAR2(255 BYTE) NOT NULL ENABLE
               ) %s""" % tablespaceTable

        self.create["d_ms_type_seq"] = \
          """CREATE SEQUENCE ms_type_seq
               START WITH 1
               INCREMENT BY 1
               NOMAXVALUE"""

        self.create["d_ms_type_tr"] = \
          """CREATE TRIGGER ms_type_tr
               BEFORE INSERT ON ms_type
               REFERENCING NEW AS NEW
               FOR EACH ROW
                 DECLARE m_no INTEGER;
                 BEGIN
                   SELECT ms_type_seq.nextval INTO :new.typeid FROM dual;
                 END;"""

        self.indexes["d_pk_ms_type"] = \
          """ALTER TABLE ms_type ADD
               (CONSTRAINT ms_type_pk PRIMARY KEY (typeid) %s)""" % tablespaceIndex

        self.indexes["d_uk_ms_type"] = \
          """ALTER TABLE ms_type ADD
               (CONSTRAINT ms_type_uk UNIQUE (name) %s)""" % tablespaceIndex

        self.create["e_ms_history"] = \
          """CREATE TABLE ms_history (
               messageid NUMBER(11)                             NOT NULL ENABLE,
               type      NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               source    NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               dest      NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               payload   CLOB                                   NOT NULL ENABLE,
               delay     VARCHAR2(50) DEFAULT '00:00:00'        NOT NULL ENABLE,
               time      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP NOT NULL ENABLE
               ) %s""" % tablespaceTable

        self.create["e_ms_history_seq"] = \
          """CREATE SEQUENCE ms_history_seq
               START WITH 1
               INCREMENT BY 1
               NOMAXVALUE"""

        self.create["e_ms_history_tr"] = \
          """CREATE TRIGGER ms_history_tr
               BEFORE INSERT ON ms_history
               REFERENCING NEW AS NEW
               FOR EACH ROW
                 DECLARE m_no INTEGER;
                 BEGIN
                   SELECT ms_history_seq.nextval INTO :new.messageid FROM dual;
                END;"""

        self.create["e_ms_history_timetrig"] = \
          """CREATE TRIGGER ms_history_timetrig
               BEFORE UPDATE ON ms_history
               REFERENCING NEW AS NEW
               FOR EACH ROW
                 BEGIN
                   SET NEW.time = CURRENT_TIMESTAMP;
                 END;"""

        self.indexes["e_ms_history_pk"] = \
          """ALTER TABLE ms_history ADD
               (CONSTRAINT e_ms_history_pk PRIMARY KEY (messageid) %s)""" % tablespaceIndex

        self.constraints["e_ms_history_type"] = \
          """ALTER TABLE ms_history ADD
               (CONSTRAINT e_ms_history_type FOREIGN KEY (type) REFERENCES ms_type(typeid))"""

        self.constraints["e_ms_history_source"] = \
          """ALTER TABLE ms_history ADD
               (CONSTRAINT e_ms_history_source FOREIGN KEY (source) REFERENCES ms_process(procid))"""

        self.constraints["e_ms_history_dest"] = \
          """ALTER TABLE ms_history ADD
               (CONSTRAINT e_ms_history_dest FOREIGN KEY (dest) REFERENCES ms_process(procid))"""

        self.create["f_ms_history_buffer"] = \
          """CREATE TABLE ms_history_buffer (
               messageid  NUMBER(11)                             NOT NULL ENABLE,
               type       NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               source     NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               dest       NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               payload    CLOB                                   NOT NULL ENABLE,
               delay      VARCHAR2(50) DEFAULT '00:00:00'        NOT NULL ENABLE,
               time       TIMESTAMP    DEFAULT CURRENT_TIMESTAMP NOT NULL ENABLE
               ) %s""" % tablespaceTable

        self.create["f_ms_history_buffer_seq"] = \
          """CREATE SEQUENCE ms_history_buffer_seq
               START WITH 1
               INCREMENT BY 1
               NOMAXVALUE"""

        self.create["f_ms_history_buffer_tr"] = \
          """CREATE TRIGGER ms_history_buffer_tr
               BEFORE INSERT ON ms_history_buffer
               REFERENCING NEW AS NEW
               FOR EACH ROW
                 DECLARE m_no INTEGER;
                 BEGIN
                  SELECT ms_history_buffer_seq.nextval INTO :new.messageid FROM dual;
                END;"""

        self.create["f_ms_history_buffer_timetrig"] = \
          """CREATE TRIGGER ms_history_buffer_timetrig
               BEFORE UPDATE ON ms_history_buffer
               REFERENCING NEW AS NEW
               FOR EACH ROW
                 BEGIN
                   SET NEW.time = CURRENT_TIMESTAMP;
                 END;"""

        self.indexes["f_ms_history_buffer_pk"] = \
          """ALTER TABLE ms_history_buffer ADD                                     
               (CONSTRAINT ms_history_buffer_pk PRIMARY KEY (messageid) %s)""" % tablespaceIndex

        self.constraints["f_ms_history_buffer_type"] = \
          """ALTER TABLE ms_history_buffer ADD
               (CONSTRAINT ms_history_buffer_type FOREIGN KEY (type) REFERENCES ms_type(typeid))"""

        self.constraints["f_ms_history_buffer_dest"] = \
          """ALTER TABLE ms_history_buffer ADD
               (CONSTRAINT ms_history_buffer_dest FOREIGN KEY (dest) REFERENCES ms_process(procid))"""

        self.constraints["f_ms_history_buffer_source"] = \
          """ALTER TABLE ms_history_buffer ADD
               (CONSTRAINT ms_history_buffer_source FOREIGN KEY (source) REFERENCES ms_process(procid))"""

        self.create["g_ms_history_priority"] = \
          """CREATE TABLE ms_history_priority (
               messageid NUMBER(11)                             NOT NULL ENABLE,
               type      NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               source    NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               dest      NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               payload   CLOB                                   NOT NULL ENABLE,
               time      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP NOT NULL ENABLE,
               delay     VARCHAR2(50) DEFAULT '00:00:00'        NOT NULL ENABLE
               ) %s""" % tablespaceTable

        self.create["g_ms_history_priority_seq"] = \
          """CREATE SEQUENCE ms_history_priority_seq
               START WITH 1
               INCREMENT BY 1
               NOMAXVALUE"""
        
        self.create["g_ms_history_priority_tr"] = \
          """CREATE TRIGGER ms_history_priority_tr
               BEFORE INSERT ON ms_history_priority
               REFERENCING NEW AS NEW
               FOR EACH ROW
                 DECLARE m_no INTEGER;
                 BEGIN
                   SELECT ms_history_priority_seq.nextval INTO :new.messageid FROM dual;
                 END; """

        self.create["g_ms_history_priority_timetrig"] = \
          """CREATE TRIGGER ms_history_priority_timetrig
               BEFORE UPDATE ON ms_history_priority
               REFERENCING NEW AS NEW
               FOR EACH ROW
                 BEGIN
                   SET NEW.time = CURRENT_TIMESTAMP;
                 END;"""

        self.indexes["g_ms_history_priority_pk"] = \
          """ALTER TABLE ms_history_priority ADD
               (CONSTRAINT ms_history_priority_pk PRIMARY KEY (messageid) %s)""" % tablespaceIndex

        self.constraints["g_ms_history_priority_type"] = \
          """ALTER TABLE ms_history_priority ADD
               (CONSTRAINT ms_history_priority_type FOREIGN KEY (type) REFERENCES ms_type(typeid))"""

        self.constraints["g_ms_history_priority_dest"] = \
          """ALTER TABLE ms_history_priority ADD
               (CONSTRAINT ms_history_priority_dest FOREIGN KEY (dest) REFERENCES ms_process(procid))"""

        self.constraints["g_ms_history_priority_source"] = \
          """ALTER TABLE ms_history_priority ADD
               (CONSTRAINT ms_history_priority_source FOREIGN KEY (source) REFERENCES ms_process(procid))"""

        self.create["h_ms_history_priority_buffer"] = \
          """CREATE TABLE ms_history_priority_buffer (
               messageid NUMBER(11)                             NOT NULL ENABLE,
               type      NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               source    NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               dest      NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               payload   CLOB                                   NOT NULL ENABLE,
               time      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP NOT NULL ENABLE,
               delay     VARCHAR2(50) DEFAULT '00:00:00'        NOT NULL ENABLE
               ) %s""" % tablespaceTable

        self.create["h_ms_history_priority_buffer_seq"] = \
          """CREATE SEQUENCE ms_history_priority_buff_seq
               START WITH 1
               INCREMENT BY 1
               NOMAXVALUE"""

        self.create["h_ms_history_priority_buffer_tr"] = \
          """CREATE TRIGGER ms_history_priority_buff_tr
               BEFORE INSERT ON ms_history_priority_buffer
               REFERENCING NEW AS NEW
               FOR EACH ROW
                 DECLARE m_no INTEGER;
                 BEGIN
                   SELECT ms_history_priority_buff_seq.nextval INTO :new.messageid FROM dual;
                 END;"""

        self.create["h_ms_history_priority_buffer_timetrig"] = \
          """CREATE TRIGGER ms_history_prio_buff_timetrig
               BEFORE UPDATE ON ms_history_priority_buffer
               REFERENCING NEW AS NEW
               FOR EACH ROW
                 BEGIN
                   SET NEW.time = CURRENT_TIMESTAMP;
                 END;"""

        self.indexes["h_ms_history_priority_buffer_pk"] = \
          """ALTER TABLE ms_history_priority_buffer ADD
               (CONSTRAINT ms_history_priority_buff_pk PRIMARY KEY (messageid) %s)""" % tablespaceIndex

        self.constraints["h_ms_history_priority_buffer_type"] = \
          """ALTER TABLE ms_history_priority_buffer ADD
               (CONSTRAINT ms_history_priority_buff_type FOREIGN KEY (type) REFERENCES ms_type(typeid))"""

        self.constraints["h_ms_history_priority_buffer_dest"] = \
          """ALTER TABLE ms_history_priority_buffer ADD
               (CONSTRAINT ms_history_priority_buff_dest FOREIGN KEY (dest) REFERENCES ms_process(procid))"""

        self.constraints["h_ms_history_priority_buffer_src"] = \
          """ALTER TABLE ms_history_priority_buffer ADD
               (CONSTRAINT ms_history_priority_buff_src FOREIGN KEY (source) REFERENCES ms_process(procid))"""

        self.create["i_ms_message"] = \
          """CREATE TABLE ms_message (
               messageid NUMBER(11)                             NOT NULL ENABLE,
               type      NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               source    NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               dest      NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               payload   CLOB                                   NOT NULL ENABLE,
               time      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP NOT NULL ENABLE,
               delay     VARCHAR2(50) DEFAULT '00:00:00'        NOT NULL ENABLE
               ) %s""" % tablespaceTable

        self.create["i_ms_message_seq"] = \
          """CREATE SEQUENCE ms_message_seq
               START WITH 1
               INCREMENT BY 1
               NOMAXVALUE"""

        self.create["i_ms_message_tr"] = \
          """CREATE TRIGGER ms_message_tr
               BEFORE INSERT ON ms_message
               REFERENCING NEW AS NEW
               FOR EACH ROW
                 DECLARE m_no INTEGER;
                 BEGIN
                   SELECT ms_message_seq.nextval INTO :new.messageid FROM dual;
                 END;"""

        self.create["i_ms_message_timetrig"] = \
          """CREATE TRIGGER ms_message_timetrig
               BEFORE UPDATE ON ms_message
               REFERENCING NEW AS NEW
               FOR EACH ROW
                 BEGIN
                   SET NEW.time = CURRENT_TIMESTAMP;
                 END;"""

        self.indexes["i_ms_message_pk"] = \
          """ALTER TABLE ms_message ADD
               (CONSTRAINT ms_message_p PRIMARY KEY (messageid) %s)""" % tablespaceIndex
        
        self.constraints["i_ms_message_type"] = \
          """ALTER TABLE ms_message ADD
               (CONSTRAINT ms_message_type FOREIGN KEY (type) REFERENCES ms_type(typeid))"""

        self.constraints["i_ms_message_dest"] = \
          """ALTER TABLE ms_message ADD
               (CONSTRAINT ms_message_dest FOREIGN KEY (dest) REFERENCES ms_process(procid))"""

        self.constraints["i_ms_message_souce"] = \
          """ALTER TABLE ms_message ADD
               (CONSTRAINT ms_message_source FOREIGN KEY (source) REFERENCES ms_process(procid))"""

        self.create["j_ms_message_buffer_in"] = \
          """CREATE TABLE ms_message_buffer_in (
               messageid NUMBER(11)                             NOT NULL ENABLE,
               type      NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               source    NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               dest      NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               payload   CLOB                                   NOT NULL ENABLE,
               time      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP NOT NULL ENABLE,
               delay     VARCHAR2(50) DEFAULT '00:00:00'        NOT NULL ENABLE
               ) %s""" % tablespaceTable

        self.create["j_ms_message_buffer_in_seq"] = \
          """CREATE SEQUENCE ms_message_buffer_in_seq
               START WITH 1
               INCREMENT BY 1
               NOMAXVALUE"""

        self.create["j_ms_message_buffer_in_tr"] = \
          """CREATE TRIGGER ms_message_buffer_in_tr
               BEFORE INSERT ON ms_message_buffer_in
               REFERENCING NEW AS NEW
               FOR EACH ROW
                 DECLARE m_no INTEGER;
                 BEGIN
                   SELECT ms_message_buffer_in_seq.nextval INTO :new.messageid FROM dual;
                 END;"""

        self.create["j_ms_message_buffer_in_timetrig"] = \
          """CREATE TRIGGER ms_message_buffer_in_timetrig
               BEFORE UPDATE ON ms_message_buffer_in
               REFERENCING NEW AS NEW
               FOR EACH ROW
                 BEGIN
                   SET NEW.time = CURRENT_TIMESTAMP;
                 END;"""

        self.indexes["j_ms_message_buffer_in_pk"] = \
          """ALTER TABLE ms_message_buffer_in ADD
               (CONSTRAINT ms_message_buffer_in_pk PRIMARY KEY (messageid) %s)""" % tablespaceIndex

        self.constraints["j_ms_message_buffer_in_type"] = \
          """ALTER TABLE ms_message_buffer_in ADD
               (CONSTRAINT ms_message_buffer_in_type FOREIGN KEY (type) REFERENCES ms_type(typeid))"""

        self.constraints["j_ms_message_buffer_in_dest"] = \
          """ALTER TABLE ms_message_buffer_in ADD
               (CONSTRAINT ms_message_buffer_in_dest FOREIGN KEY (dest) REFERENCES ms_process(procid))"""

        self.constraints["j_ms_message_buffer_in_source"] = \
          """ALTER TABLE ms_message_buffer_in ADD
               (CONSTRAINT ms_message_buffer_in_source FOREIGN KEY (source) REFERENCES ms_process(procid))"""

        self.create["k_ms_message_buffer_out"] = \
          """CREATE TABLE ms_message_buffer_out (
               messageid NUMBER(11)                             NOT NULL ENABLE,
               type      NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               source    NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               dest      NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               payload   CLOB                                   NOT NULL ENABLE,
               time      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP NOT NULL ENABLE,
               delay     VARCHAR2(50) DEFAULT '00:00:00'        NOT NULL ENABLE,
               state     VARCHAR2(20) DEFAULT 'wait'
               ) %s""" % tablespaceTable

        self.create["k_ms_message_buffer_out_seq"] = \
          """CREATE SEQUENCE ms_message_buffer_out_seq
               START WITH 1
               INCREMENT BY 1
               NOMAXVALUE"""

        self.create["k_ms_message_buffer_out_tr"] = \
          """CREATE TRIGGER ms_message_buffer_out_tr
               BEFORE INSERT ON ms_message_buffer_out
               REFERENCING NEW AS NEW
               FOR EACH ROW
                 DECLARE m_no INTEGER;
                   BEGIN
                     SELECT ms_message_buffer_out_seq.nextval INTO :new.messageid FROM dual;
                  END;"""

        self.create["k_ms_message_buffer_out_timetrig"] = \
          """CREATE TRIGGER ms_message_buffer_out_timetrig
               BEFORE UPDATE ON ms_message_buffer_out
               REFERENCING NEW AS NEW
               FOR EACH ROW
                 BEGIN
                   :NEW.time := CURRENT_TIMESTAMP;
                 END;"""

        self.indexes["k_ms_message_buffer_out_pk"] = \
          """ALTER TABLE ms_message_buffer_out ADD
               (CONSTRAINT ms_message_buffer_out_pk PRIMARY KEY (messageid) %s)""" % tablespaceIndex

        self.constraints["k_ms_message_buffer_out_type"] = \
          """ALTER TABLE ms_message_buffer_out ADD
               (CONSTRAINT ms_message_buffer_out_type FOREIGN KEY (type) REFERENCES ms_type(typeid))"""

        self.constraints["k_ms_message_buffer_out_dest"] = \
          """ALTER TABLE ms_message_buffer_out ADD
               (CONSTRAINT ms_message_buffer_out_dest FOREIGN KEY (dest) REFERENCES ms_process(procid))"""

        self.constraints["k_ms_message_buffer_out_source"] = \
          """ALTER TABLE ms_message_buffer_out ADD
               (CONSTRAINT ms_message_buffer_out_source FOREIGN KEY (source) REFERENCES ms_process(procid))"""

        self.create["l_ms_priority_message"] = \
          """CREATE TABLE ms_priority_message (
               messageid NUMBER(11)                             NOT NULL ENABLE,
               type      NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               source    NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               dest      NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               payload   CLOB                                   NOT NULL ENABLE,
               time      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP NOT NULL ENABLE,
               delay     VARCHAR2(50) DEFAULT '00:00:00'        NOT NULL ENABLE
               ) %s""" % tablespaceTable

        self.create["l_ms_priority_message_seq"] = \
          """CREATE SEQUENCE ms_priority_message_seq
               START WITH 1
               INCREMENT BY 1
               NOMAXVALUE"""

        self.create["l_ms_priority_message_tr"] = \
          """CREATE TRIGGER ms_priority_message_tr
               BEFORE INSERT ON ms_priority_message
               REFERENCING NEW AS NEW
               FOR EACH ROW
                 DECLARE m_no INTEGER;
                 BEGIN
                   SELECT ms_priority_message_seq.nextval INTO :new.messageid FROM dual;
                 END;"""

        self.create["l_ms_priority_message_timetrig"] = \
          """CREATE TRIGGER ms_priority_message_timetrig
               BEFORE UPDATE ON ms_priority_message
               REFERENCING NEW AS NEW
               FOR EACH ROW
                 BEGIN
                   SET NEW.time = CURRENT_TIMESTAMP;
                 END;"""

        self.indexes["l_ms_priority_message_pk"] = \
          """ALTER TABLE ms_priority_message ADD
               (CONSTRAINT ms_priority_message_pk PRIMARY KEY (messageid) %s)""" % tablespaceIndex

        self.constraints["l_ms_priority_message_type"] = \
          """ALTER TABLE ms_priority_message ADD
               (CONSTRAINT ms_priority_message_type FOREIGN KEY (type) REFERENCES ms_type(typeid))"""

        self.constraints["l_ms_priority_message_type"] = \
          """ALTER TABLE ms_priority_message ADD
               (CONSTRAINT ms_priority_message_dest FOREIGN KEY (dest) REFERENCES ms_process(procid))"""

        self.constraints["l_ms_priority_message_type"] = \
          """ALTER TABLE ms_priority_message ADD
               (CONSTRAINT ms_priority_message_source FOREIGN KEY (source) REFERENCES ms_process(procid))"""

        self.create["m_ms_priority_message_buffer_in"] = \
          """CREATE TABLE ms_priority_message_buffer_in (
               messageid NUMBER(11)                             NOT NULL ENABLE,
               type      NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               source    NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               dest      NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               payload   CLOB                                   NOT NULL ENABLE,
               time      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP NOT NULL ENABLE,
               delay     VARCHAR2(50) DEFAULT '00:00:00'        NOT NULL ENABLE
               ) %s""" % tablespaceTable

        self.create["m_ms_priority_message_buffer_in_seq"] = \
          """CREATE SEQUENCE ms_prio_message_buff_in_seq
               START WITH 1
               INCREMENT BY 1
               NOMAXVALUE"""

        self.create["m_ms_priority_message_buffer_in_tr"] = \
          """CREATE TRIGGER ms_prio_message_buff_in_tr
               BEFORE INSERT ON ms_priority_message_buffer_in
               REFERENCING NEW AS NEW
               FOR EACH ROW
                 DECLARE m_no INTEGER;
                   BEGIN
                     SELECT ms_prio_message_buff_in_seq.nextval INTO :new.messageid FROM dual;
                   END;"""

        self.create["m_ms_priority_message_buffer_in_timetrig"] = \
          """CREATE TRIGGER ms_prio_msg_buff_in_timetrig
               BEFORE UPDATE ON ms_priority_message_buffer_in
               REFERENCING NEW AS NEW
               FOR EACH ROW
                 BEGIN
                   SET NEW.time = CURRENT_TIMESTAMP;
                 END;"""

        self.indexes["m_ms_priority_message_buffer_in_pk"] = \
          """ALTER TABLE ms_priority_message_buffer_in ADD
               (CONSTRAINT ms_prio_message_buff_in_pk PRIMARY KEY (messageid) %s)""" % tablespaceIndex

        self.constraints["m_ms_prio_message_buff_in_type"] = \
          """ALTER TABLE ms_priority_message_buffer_in ADD
               (CONSTRAINT ms_prio_message_buff_in_type FOREIGN KEY (type) REFERENCES ms_type(typeid))"""

        self.constraints["m_ms_prio_message_buff_in_dest"] = \
          """ALTER TABLE ms_priority_message_buffer_in ADD
               (CONSTRAINT ms_prio_message_buff_in_dest FOREIGN KEY (dest) REFERENCES ms_process(procid))"""

        self.constraints["m_ms_prio_message_buff_in_source"] = \
          """ALTER TABLE ms_priority_message_buffer_in ADD
               (CONSTRAINT ms_prio_message_buff_in_source FOREIGN KEY (source) REFERENCES ms_process(procid))"""

        self.create["n_ms_priority_message_buffer_out"] = \
          """CREATE TABLE ms_priority_message_buffer_out (
               messageid NUMBER(11)                             NOT NULL ENABLE,
               type      NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               source    NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               dest      NUMBER(11)   DEFAULT '0'               NOT NULL ENABLE,
               payload   CLOB                                   NOT NULL ENABLE,
               time      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP NOT NULL ENABLE,
               delay     VARCHAR2(50) DEFAULT '00:00:00'        NOT NULL ENABLE,
               state     VARCHAR2(20) DEFAULT 'wait'
               ) %s""" % tablespaceTable

        self.create["n_ms_priority_message_buffer_out_seq"] = \
          """CREATE SEQUENCE ms_prio_msg_buff_out_seq
               START WITH 1
               INCREMENT BY 1
               NOMAXVALUE"""

        self.create["n_ms_priority_message_buffer_out_tr"] = \
          """CREATE TRIGGER ms_prio_msg_buff_out_tr
               BEFORE INSERT ON ms_priority_message_buffer_out
               REFERENCING NEW AS NEW
               FOR EACH ROW
                 DECLARE m_no INTEGER;
                 BEGIN
                   SELECT ms_prio_msg_buff_out_seq.nextval INTO :new.messageid FROM dual;
                 END;"""

        self.create["n_ms_priority_message_buffer_out_timetrig"] = \
          """CREATE TRIGGER ms_prio_msg_buff_out_timetrig
               BEFORE UPDATE ON ms_priority_message_buffer_out
               REFERENCING NEW AS NEW
               FOR EACH ROW
                 BEGIN
                   :NEW.time := CURRENT_TIMESTAMP;
                 END;"""

        self.indexes["n_ms_prio_msg_buffer_out_pk"] = \
          """ALTER TABLE ms_priority_message_buffer_out ADD                                          
               (CONSTRAINT ms_prio_msg_buff_out_pk PRIMARY KEY (messageid) %s)""" % tablespaceIndex

        self.constraints["n_ms_prio_msg_buff_out_type"] = \
          """ALTER TABLE ms_priority_message_buffer_out ADD
               (CONSTRAINT ms_prio_msg_buff_out_state CHECK(state IN ('queued', 'processing', 'wait')))"""

        self.constraints["n_ms_prio_msg_buff_out_type"] = \
          """ALTER TABLE ms_priority_message_buffer_out ADD        
               (CONSTRAINT ms_prio_msg_buff_out_type FOREIGN KEY (type) REFERENCES ms_type(typeid))"""

        self.constraints["n_ms_prio_msg_buff_out_type"] = \
          """ALTER TABLE ms_priority_message_buffer_out ADD
               (CONSTRAINT ms_prio_msg_buff_out_dest FOREIGN KEY (dest) REFERENCES ms_process(procid))"""

        self.constraints["n_ms_prio_msg_buff_out_type"] = \
          """ALTER TABLE ms_priority_message_buffer_out ADD
               (CONSTRAINT ms_prio_msg_buff_out_source FOREIGN KEY (source) REFERENCES ms_process(procid))"""

        self.create["o_ms_subscription"] = \
          """CREATE TABLE ms_subscription (
               subid  NUMBER(11)             NOT NULL ENABLE,
               procid NUMBER(11) DEFAULT '0' NOT NULL ENABLE,
               typeid NUMBER(11) DEFAULT '0' NOT NULL ENABLE
               ) %s""" % tablespaceTable

        self.create["o_ms_subscription_seq"] = \
          """CREATE SEQUENCE ms_subscription_seq
               START WITH 1
               INCREMENT BY 1
               NOMAXVALUE"""

        self.create["o_ms_subscription_tr"] = \
          """CREATE TRIGGER ms_subscription_tr
               BEFORE INSERT ON ms_subscription
               REFERENCING NEW AS NEW
               FOR EACH ROW
                 DECLARE m_no INTEGER;
                 BEGIN
                   SELECT ms_subscription_seq.nextval INTO :new.subid FROM dual;
                 END;"""

        self.indexes["o_ms_subscription_pk"] = \
          """ALTER TABLE ms_subscription ADD
               (CONSTRAINT ms_subscription_pk PRIMARY KEY (subid) %s)""" % tablespaceIndex

        self.indexes["o_ms_subscription_uk"] = \
          """ALTER TABLE ms_subscription ADD
               (CONSTRAINT ms_subscription_unique UNIQUE(procid, typeid) %s)""" % tablespaceIndex

        self.constraints["o_ms_subscription_procid"] = \
          """ALTER TABLE ms_subscription ADD
               (CONSTRAINT ms_subscription_procid FOREIGN KEY (procid) REFERENCES ms_process(procid))"""

        self.constraints["o_ms_subscription_procid"] = \
          """ALTER TABLE ms_subscription ADD
               (CONSTRAINT ms_subscription_typeid  FOREIGN KEY (typeid) REFERENCES ms_type (typeid))"""

        self.create["p_ms_subscription_priority"] = \
          """CREATE TABLE ms_subscription_priority (
               subid  NUMBER(11)             NOT NULL ENABLE,
               procid NUMBER(11) DEFAULT '0' NOT NULL ENABLE,
               typeid NUMBER(11) DEFAULT '0' NOT NULL ENABLE
               ) %s""" % tablespaceTable

        self.create["p_ms_subscription_priority_seq"] = \
          """CREATE SEQUENCE ms_subscription_prio_seq
               START WITH 1
               INCREMENT BY 1
               NOMAXVALUE"""

        self.create["p_ms_subscription_priority_tr"] = \
          """CREATE TRIGGER ms_subscription_prio_tr
               BEFORE INSERT ON ms_subscription_priority
               REFERENCING NEW AS NEW
               FOR EACH ROW
                 DECLARE m_no INTEGER;
                 BEGIN
                   SELECT ms_subscription_prio_seq.nextval INTO :new.subid FROM dual;
                 END;"""

        self.indexes["p_ms_subscription_prio_pk"] = \
          """ALTER TABLE ms_subscription_priority ADD
               (CONSTRAINT ms_subscription_prio_pk PRIMARY KEY (subid) %s)""" % tablespaceIndex

        self.indexes["p_ms_subscription_prio_uk"] = \
          """ALTER TABLE ms_subscription_priority ADD
               (CONSTRAINT ms_subscription_prio_unique UNIQUE(procid, typeid) %s)""" % tablespaceIndex

        self.constraints["p_ms_subscription_prio_procid"] = \
          """ALTER TABLE ms_subscription_priority ADD
               (CONSTRAINT ms_subscription_prio_procid FOREIGN KEY (procid) REFERENCES ms_process(procid))"""

        self.constraints["p_ms_subscription_prio_procid"] = \
          """ALTER TABLE ms_subscription_priority ADD
               (CONSTRAINT ms_subscription_prio_typeid FOREIGN KEY (typeid) REFERENCES ms_type (typeid))"""

        self.create["q_ms_available"] = \
          """CREATE TABLE ms_available (
               procid   NUMBER(11)   NOT NULL ENABLE,
               status   VARCHAR2(20) DEFAULT 'not_there'
               ) %s""" % tablespaceIndex

        self.indexes["q_ms_available_pk"] = \
          """ALTER TABLE ms_available ADD
               (CONSTRAINT ms_available_unique PRIMARY KEY (procid) %s)""" % tablespaceIndex

        self.constraints["q_ms_available_status"] = \
          """ALTER TBALE ms_available ADD
               (CONSTRAINT ms_available_status CHECK(status IN ('there', 'not_there')))"""

        self.constraints["q_ms_available_procid"] = \
          """ALTER TBALE ms_available ADD
               (CONSTRAINT ms_available_procid FOREIGN KEY (procid) REFERENCES ms_process(procid))"""

        self.create["r_ms_available_priority"] = \
          """CREATE TABLE ms_available_priority (
               procid NUMBER(11)    NOT NULL ENABLE,
               status VARCHAR2(20)  DEFAULT 'not_there'
               ) %s""" % tablespaceTable

        self.indexes["r_ms_available_priority_pk"] = \
          """ALTER TABLE ms_available_priority ADD
               (CONSTRAINT ms_available_priority_unique PRIMARY KEY (procid) %s)""" % tablespaceIndex

        self.constraints["r_ms_available_priority_status"] = \
          """ALTER TABLE ms_available_priority ADD
               (CONSTRAINT ms_available_priority_status CHECK(status IN ('there', 'not_there')))"""

        self.constraints["r_ms_available_priority_procid"] = \
          """ALTER TABLE ms_available_priority ADD
               (CONSTRAINT ms_available_priority_procid FOREIGN KEY (procid) REFERENCES ms_process(procid))"""

        self.create["s_ms_check_buffer"] = \
          """CREATE TABLE ms_check_buffer (
               buffer VARCHAR2(100) NOT NULL ENABLE,
               status VARCHAR2(20)  DEFAULT 'not_checking'
               ) %s""" % tablespaceTable

        self.indexes["s_ms_check_buffer_pk"] = \
          """ALTER TABLE ms_check_buffer ADD
               (CONSTRAINT ms_check_buffer_unique PRIMARY KEY(buffer) %s)""" % tablespaceIndex

        self.constraints["s_ms_check_buffer_status"] = \
          """ALTER TABLE ms_check_buffer ADD
               (CONSTRAINT ms_check_buffer_status CHECK(status IN ('checking', 'not_checking')))"""

