"""
_Create_DBSBuffer_

Implementation of Create_DBSBuffer for Oracle.
"""

__revision__ = "$Id: Create.py,v 1.1 2009/05/15 16:19:13 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"

import logging
import threading

#Example in WMCore/MsgService/MySQL/Create.py

from WMCore.Database.DBCreator import DBCreator

class Create(DBCreator):

    def __init__(self):
        """
        _init_

        Call the base class's constructor and create all necessary tables,
        constraints and inserts.
        """
        myThread = threading.currentThread()
        DBCreator.__init__(self, myThread.logger, myThread.dbi)

        #dbsbuffer_dataset
        
        self.create["01dbsbuffer_dataset"] ="""
CREATE TABLE dbsbuffer_dataset
     (
	ID                    NUMBER(11)      NOT NULL ENABLE,
	Path                  varchar2(500)   NOT NULL ENABLE,
        UnMigratedFiles       NUMBER          Default 0,
        Algo                  NUMBER,
        AlgoInDBS             INTEGER, 
        LastModificationDate  NUMBER(11),
        CONSTRAINT dbsbuffer_dataset_unique UNIQUE (Path),
        CONSTRAINT dbsbuffer_dataset_pk     PRIMARY KEY (ID)	
     )"""

        self.create['01dbsbuffer_dataset_seq'] = """
CREATE SEQUENCE dbsbuffer_dataset_seq
        start with 1
        increment by 1
        nomaxvalue
"""

        self.create['01dbsbuffer_dataset_trg'] = """
CREATE TRIGGER dbsbuffer_dataset_trg
BEFORE INSERT ON dbsbuffer_dataset
FOR EACH ROW
     BEGIN
        SELECT dbsbuffer_dataset_seq.nextval INTO :NEW.ID FROM dual;
     END;        """

        
        #dbsbuffer_algo

        self.create["03dbsbuffer_algo"] = """
CREATE TABLE dbsbuffer_algo
     (
        ID       NUMBER(11)     NOT NULL ENABLE,   
        AppName  varchar2(100),
        AppVer   varchar2(100),
        AppFam   varchar2(100),
        PSetHash varchar2(700),
        ConfigContent LONG,
        LastModificationDate  NUMBER(11),
        CONSTRAINT dbsbuffer_algo_unique UNIQUE (AppName,AppVer,AppFam,PSetHash),
        CONSTRAINT dbsbuffer_algo_pk     PRIMARY KEY (ID)
     )"""

        self.create['03dbsbuffer_algo_seq'] = """
CREATE SEQUENCE dbsbuffer_algo_seq
        start with 1
        increment by 1
        nomaxvalue
"""

        self.create['03dbsbuffer_algo_trg'] = """
CREATE TRIGGER dbsbuffer_algo_trg
BEFORE INSERT ON dbsbuffer_algo
REFERENCING NEW AS NEW
FOR EACH ROW
     DECLARE m_no INTEGER;
     BEGIN
        SELECT dbsbuffer_algo_seq.nextval INTO :new.ID FROM dual;
     END;        """


        #dbsbuffer_file

        self.create["04dbsbuffer_file"] = """
CREATE TABLE dbsbuffer_file (
     id                    NUMBER(11)    NOT NULL ENABLE,
     lfn                   VARCHAR2(255) NOT NULL ENABLE,
     "size"                NUMBER(11),
     events                INTEGER,
     cksum                 NUMBER(11),
     dataset 	           NUMBER(11)    NOT NULL ENABLE,
     status                varchar2(20),
     first_event           INTEGER,
     last_event            INTEGER,
     LastModificationDate  NUMBER(11),

     CONSTRAINT dbsbuffer_file_pk     PRIMARY KEY (id)
     )"""

        self.create['04dbsbuffer_file_seq'] = """
CREATE SEQUENCE dbsbuffer_file_seq
        start with 1
        increment by 1
        nomaxvalue
"""

        self.create['04dbsbuffer_file_trg'] = """
CREATE TRIGGER dbsbuffer_file_trg
BEFORE INSERT ON dbsbuffer_file
REFERENCING NEW AS NEW
FOR EACH ROW
     DECLARE m_no INTEGER;
     BEGIN
        SELECT dbsbuffer_file_seq.nextval INTO :new.id FROM dual;
     END;        """



        #dbsbuffer_file_parent
        
        self.create["05dbsbuffer_file_parent"] = """
CREATE TABLE dbsbuffer_file_parent (
     child  CONSTRAINT dbsbuffer_file_parent_child  REFERENCES dbsbuffer_file(id)
     ON DELETE CASCADE,
     parent CONSTRAINT dbsbuffer_file_parent_parent REFERENCES dbsbuffer_file(id),
     CONSTRAINT dbsbuffer_file_parent_unique UNIQUE(child, parent)
     )"""

        #dbsbuffer_file_runlumi_map

        self.create["06dbsbuffer_file_runlumi_map"] = """
CREATE TABLE dbsbuffer_file_runlumi_map (
     "file"  CONSTRAINT dbsbuffer_file_runlumi_map_i REFERENCES dbsbuffer_file(id)
     ON DELETE CASCADE,
     run     INTEGER    NOT NULL ENABLE,
     lumi    INTEGER    NOT NULL ENABLE
     )"""


        #dbsbuffer_location

        self.create["07dbsbuffer_location"] = """
CREATE TABLE dbsbuffer_location (
     id      INTEGER       NOT NULL ENABLE,
     se_name VARCHAR2(255) NOT NULL ENABLE,
     CONSTRAINT dbsbuffer_location_unique UNIQUE (se_name),
     CONSTRAINT dbsbuffer_location_pk     PRIMARY KEY (id)
     )"""

        self.create['07dbsbuffer_location_seq'] = """
CREATE SEQUENCE dbsbuffer_location_seq
        start with 1
        increment by 1
        nomaxvalue
"""

        self.create['07dbsbuffer_location_trg'] = """
CREATE TRIGGER dbsbuffer_location_trg
BEFORE INSERT ON dbsbuffer_location
REFERENCING NEW AS NEW
FOR EACH ROW
     DECLARE m_no INTEGER;
     BEGIN
        SELECT dbsbuffer_location_seq.nextval INTO :new.id FROM dual;
     END;        """



        #dbsbuffer_file_location

        self.create["08dbsbuffer_file_location"] = """
CREATE TABLE dbsbuffer_file_location (
     "file"     CONSTRAINT dbsbuffer_file_location_file REFERENCES dbsbuffer_file(id)
     ON DELETE CASCADE,
     location   CONSTRAINT dbsbuffer_file_location_loc REFERENCES
     dbsbuffer_location(id) ON DELETE CASCADE,
     CONSTRAINT dbsbuffer_file_location_uniq UNIQUE ("file", location)
     )"""

        

        self.constraints["FK_dbsbuffer_file_ds"]=\
		      """ALTER TABLE dbsbuffer_file ADD CONSTRAINT FK_dbsbuffer_file_ds
    			 foreign key(Dataset) references dbsbuffer_dataset(ID) on delete CASCADE"""

        self.constraints["FK_dbsbuffer_file_ds"]=\
                      """ALTER TABLE dbsbuffer_file ADD CONSTRAINT FK_dbsbuffer_dbsbuffile
                         foreign key(WMBS_File_ID) references dbsbuffer_file(ID)"""

        self.constraints["FK_dbsbuffer_ds_algo"]=\
              """ALTER TABLE dbsbuffer_algo DD CONSTRAINT FK_dbsbuffer_ds_algo
                 foreign key(Algo) references dbsbuffer_algo(ID)"""
                 
	#self.triggers IS NOT a member so I will just use self.create for now
        self.create["09TR_dbsbuffer_file_lud"]=\
                """CREATE TRIGGER TR_dbsbuffer_file_lud BEFORE INSERT ON dbsbuffer_file
                        REFERENCING NEW AS NEW
                        FOR EACH ROW
                        BEGIN
                             SELECT FLOOR( (CURRENT_DATE - TO_DATE('01-JAN-1970 00:00:00', 'DD-MON-YYYY HH24:MI:SS') )*(24*60*60))  - (TO_NUMBER(SUBSTR(SESSIONTIMEZONE,1,3))*60*60) INTO :NEW.LastModificationDate FROM dual;
                        END; """

        self.create["10TR_dbsbuffer_ds_lud"]=\
                """CREATE TRIGGER TR_dbsbuffer_ds_lud BEFORE INSERT ON dbsbuffer_dataset
                        REFERENCING NEW AS NEW
                        FOR EACH ROW
                        BEGIN
                             SELECT FLOOR( (CURRENT_DATE - TO_DATE('01-JAN-1970 00:00:00', 'DD-MON-YYYY HH24:MI:SS') )*(24*60*60))  - (TO_NUMBER(SUBSTR(SESSIONTIMEZONE,1,3))*60*60) INTO :NEW.LastModificationDate FROM dual;
                        END; """

                        
        self.create["11TR_dbsbuffer_algo_lud"]= """
        CREATE TRIGGER TR_dbsbuffer_algo_lud BEFORE INSERT ON dbsbuffer_algo
        REFERENCING NEW AS NEW
        FOR EACH ROW
           BEGIN
             SELECT FLOOR( (CURRENT_DATE - TO_DATE('01-JAN-1970 00:00:00', 'DD-MON-YYYY HH24:MI:SS') )*(24*60*60))  - (TO_NUMBER(SUBSTR(SESSIONTIMEZONE,1,3))*60*60) INTO :NEW.LastModificationDate FROM dual;
           END;  """


