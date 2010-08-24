#!/usr/bin/python
#pylint: disable-msg=E1103

"""
_Create_

Class for creating MySQL specific schema for persistent messages.

"""

__revision__ = "$Id: Create.py,v 1.1 2008/10/02 11:33:03 fvlingen Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "fvlingen@caltech.edu"

import logging
import threading

from WMCore.Database.DBCreator import DBCreator

class Create(DBCreator):
    """
    _Create_
    
    Class for creating MySQL specific schema for persistent messages.
    """
    
    
    
    def __init__(self):
        myThread = threading.currentThread()
        DBCreator.__init__(self, myThread.logger, myThread.dbi)
        self.create = {}
        self.constraints = {}
        self.create['a_ms_process'] = """
CREATE TABLE ms_process
(       procid NUMBER NOT NULL ENABLE,
        name VARCHAR2(40 BYTE) NOT NULL ENABLE,
        host VARCHAR2(60 BYTE) NOT NULL ENABLE,
        pid NUMBER NOT NULL ENABLE,
        CONSTRAINT "MS_PROCESS_PK" PRIMARY KEY (procid),
        CONSTRAINT "MS_PROCESS_UK1" UNIQUE (name),
        CONSTRAINT "MS_PROCESS_UK2" UNIQUE (pid)
)
"""
        self.create['b_ms_process_seq1'] = """
CREATE SEQUENCE MS_PROCESS_SEQ1
        start with 1
        increment by 1
        nomaxvalue
"""
        self.create['c_ms_process_trig1'] = """
CREATE TRIGGER MS_PROCESS_TR1
BEFORE INSERT ON ms_process
FOR EACH ROW
    DECLARE m_no INTEGER;
BEGIN
    SELECT MS_PROCESS_SEQ1.nextval INTO :new.procid FROM dual;
END;
/
"""
        self.create['d_ms_type'] = """
CREATE TABLE ms_type
(       typeid  NUMBER NOT NULL ENABLE,
        name  VARCHAR2(255 BYTE) NOT NULL ENABLE,
        CONSTRAINT "MS_TYPE_PK" PRIMARY KEY (typeid),
        CONSTRAINT "MS_TYPE_UK1" UNIQUE (name)
)
        """
        self.create['e_ms_type_seq1'] = """
CREATE SEQUENCE MS_TYPE_SEQ1
start with 1
increment by 1
nomaxvalue
        """
        self.create['f_ms_type_tr1'] = """
CREATE TRIGGER MS_TYPE_TR1
BEFORE INSERT ON ms_type
FOR EACH ROW
    DECLARE m_no INTEGER;
BEGIN
    SELECT MS_TYPE_SEQ1.nextval INTO :new.typeid FROM dual;
END;
/
        """
