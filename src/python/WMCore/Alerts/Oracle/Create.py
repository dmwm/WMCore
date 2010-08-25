#!/usr/bin/python
"""
_Create_

Create the "alert_current" and "alert_history" tables for the alert
system in a MySQL database.
"""

__revision__ = "$Id: Create.py,v 1.1 2009/07/10 21:45:34 sryu Exp $"
__version__ = "$Revision: 1.1 $"

import threading

from WMCore.Database.DBCreator import DBCreator

class Create(DBCreator):
    """
    _Create_

    Create the "alert_current" and "alert_history" tables for the alert
    system in a MySQL database.
    """
    def __init__(self):
        """
        ___init___

        Create the "alert_current" and "alert_history" tables, nothing else is
        needed.
        """
        myThread = threading.currentThread()
        DBCreator.__init__(self, myThread.logger, myThread.dbi)
        self.create = {}
        self.constraints = {}

        self.sequenceTables = ['alert_current', 'alert_history']
    
        self.create["alert_current"] = \
          """CREATE TABLE alert_current (
             id        INTEGER       NOT NULL,
             severity  VARCHAR2(30)  NOT NULL,
             component VARCHAR2(30)  NOT NULL,
             message   VARCHAR2(900) NOT NULL,
             time      INTEGER       NOT NULL,
             PRIMARY KEY (id))"""

        self.create["alert_history"] = \
          """CREATE TABLE alert_history (
             id             INTEGER       NOT NULL,
             severity       VARCHAR2(30)  NOT NULL,
             component      VARCHAR2(30)  NOT NULL,
             message        VARCHAR2(900) NOT NULL,
             generationtime INTEGER       NOT NULL,
             historytime    INTEGER       NOT NULL,
             PRIMARY KEY (id))"""

        for tableName in self.sequenceTables:
            seqname = '%s_SEQ' % tableName
            self.create["%s" % seqname] = """
                            CREATE SEQUENCE %s start with 1 
                            increment by 1 nomaxvalue cache 100""" % seqname
            triggerName = '%s_TRG' % tableName
            self.create["%s" % triggerName] = """
                    CREATE TRIGGER %s
                        BEFORE INSERT ON %s
                        REFERENCING NEW AS newRow
                        FOR EACH ROW
                        BEGIN
                            SELECT %s.nextval INTO :newRow.id FROM dual;
                        END; """ % (triggerName, tableName, seqname)