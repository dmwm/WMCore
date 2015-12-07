#!/usr/bin/python

"""
_Create_

Class for creating Oracle specific schema for persistent messages.

"""





import threading

from WMCore.Database.DBCreator import DBCreator

class Create(DBCreator):
    """
    _Create_

    Class for creating Oracle specific schema for persistent messages.
    """

    sequence_tables = []
    sequence_tables.append('tp_threadpool_seq')
    sequence_tables.append('tp_buffer_in_seq')
    sequence_tables.append('tp_buffer_out_seq')

    trigger_tables = []
    trigger_tables.append('tp_threadpool_trg')
    trigger_tables.append('tp_buffer_in_trg')
    trigger_tables.append('tp_buffer_out_trg')

    def __init__(self, logger = None, dbi = None, params = None):
        myThread = threading.currentThread()
        DBCreator.__init__(self, myThread.logger, myThread.dbi)
        self.create = {}
        self.constraints = {}

        tablespaceTable = ""
        tablespaceIndex = ""

        if params:
            if "tablespace_table" in params:
                tablespaceTable = "TABLESPACE %s" % params["tablespace_table"]
            if "tablespace_index" in params:
                tablespaceIndex = "USING INDEX TABLESPACE %s" % params["tablespace_index"]

        self.create["tp_threadpool"] = \
          """CREATE TABLE tp_threadpool (
               id             NUMBER(11)    NOT NULL ENABLE,
               event          VARCHAR2(255) NOT NULL ENABLE,
               component      VARCHAR2(255) NOT NULL ENABLE,
               payload        CLOB          NOT NULL ENABLE,
               thread_pool_id VARCHAR2(255) NOT NULL ENABLE,
               state          VARCHAR(20)   DEFAULT 'queued'  NOT NULL ENABLE
               ) %s""" % tablespaceTable

        self.create["tp_threadpool_seq"] = \
          """CREATE SEQUENCE tp_threadpool_seq
               START WITH 1
               INCREMENT BY 1
               NOMAXVALUE"""

        self.indexes["tp_threadpool_pk"] = \
          """ALTER TABLE tp_threadpool ADD
               (CONSTRAINT tp_threadpool_pk PRIMARY KEY (id) %s)""" % tablespaceIndex

        self.constraints["tp_threadpool_fk"] = \
          """ALTER TABLE tp_threadpool ADD
               (CONSTRAINT tp_threadpool_state CHECK(state IN ('queued', 'process')))"""

        self.create["tp_threadpool_trg"] = \
          """CREATE TRIGGER tp_threadpool_trg
               BEFORE INSERT ON tp_threadpool
               FOR EACH ROW
                 DECLARE m_no INTEGER;
                 BEGIN
                   SELECT tp_threadpool_seq.nextval INTO :new.id FROM dual;
                 END;"""

        self.create["tp_threadpool_buffer_in"] = \
          """CREATE TABLE tp_threadpool_buffer_in (
               id             NUMBER(11)   NOT NULL ENABLE,
               event          VARCHAR(255) NOT NULL ENABLE,
               component      VARCHAR(255) NOT NULL ENABLE,
               payload        CLOB         NOT NULL ENABLE,
               thread_pool_id VARCHAR(255) NOT NULL ENABLE,
               state          VARCHAR2(20) DEFAULT 'queued' NOT NULL ENABLE
               ) %s""" % tablespaceTable

        self.create["tp_threadpool_buffer_in_seq"] = \
          """CREATE SEQUENCE tp_buffer_in_seq
               START WITH 1
               INCREMENT BY 1
               NOMAXVALUE"""

        self.create["tp_threadpool_buffer_in_trg"] = \
          """CREATE TRIGGER tp_buffer_in_trg
               BEFORE INSERT ON tp_threadpool_buffer_in
               FOR EACH ROW
                 BEGIN
                   SELECT tp_buffer_in_seq.nextval INTO :new.id FROM dual;
                 END;"""

        self.constraints["tp_threadpool_buffer_in_fk"] = \
          """ALTER TABLE tp_threadpool_buffer_in ADD
               (CONSTRAINT tp_threadpool_buffer_in_state CHECK(state IN ('queued', 'process')))"""

        self.indexes["tp_threadpool_buffer_in_pk"] = \
          """ALTER TABLE tp_threadpool_buffer_in ADD
               (CONSTRAINT tp_threadpool_buffer_in_pk PRIMARY KEY (id) %s)""" % tablespaceIndex

        self.create["tp_threadpool_buffer_out"] = \
          """CREATE TABLE tp_threadpool_buffer_out (
               id             NUMBER(11)   NOT NULL ENABLE,
               event          varchar(255) NOT NULL ENABLE,
               component      varchar(255) NOT NULL ENABLE,
               payload        clob         NOT NULL ENABLE,
               thread_pool_id varchar(255) NOT NULL ENABLE,
               state          varchar2(20) DEFAULT 'queued' NOT NULL ENABLE
               ) %s""" % tablespaceTable

        self.create["threadpool_buffer_out_seq"] = \
          """CREATE SEQUENCE tp_buffer_out_seq
               START WITH 1
               INCREMENT BY 1
               NOMAXVALUE"""

        self.create["tp_threadpool_buffer_out_trg"] = \
          """CREATE TRIGGER tp_buffer_out_trg
               BEFORE INSERT ON tp_threadpool_buffer_out
               FOR EACH ROW
                 BEGIN
                   SELECT tp_buffer_out_seq.nextval INTO :new.id FROM dual;
                 END;"""

        self.indexes["tp_threadpool_buffer_out_pk"] = \
          """ALTER TABLE tp_threadpool_buffer_out ADD
               (CONSTRAINT tp_threadpool_buffer_out_pk PRIMARY KEY (id) %s)""" % tablespaceIndex

        self.constraints["tp_threadpool_buffer_out_ck"] = \
          """ALTER TABLE tp_threadpool_buffer_out ADD
               (CONSTRAINT tp_threadpool_buffer_out_state CHECK(state IN ('queued', 'process')))"""
