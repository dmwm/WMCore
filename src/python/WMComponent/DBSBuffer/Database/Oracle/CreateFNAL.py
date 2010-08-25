"""
_Create_DBSBuffer_

Implementation of Create_DBSBuffer for Oracle.
"""

__revision__ = "$Id: CreateFNAL.py,v 1.1 2009/07/20 20:24:44 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

import threading

from WMCore.Database.DBCreator import DBCreator

class Create(DBCreator):

    def __init__(self):
        """
        _init_

        Call the base class's constructor and create all necessary tables,
        constraints and inserts.
        """
        params = {}
        params["tablespace_table"] = "TIER1_WMBS_DATA"
        params["tablespace_index"] = "TIER1_WMBS_INDEX"
        
        myThread = threading.currentThread()
        DBCreator.__init__(self, myThread.logger, myThread.dbi)

        self.create["01dbsbuffer_dataset"] = \
          """CREATE TABLE dbsbuffer_dataset
               (
	         id   NUMBER(11)      NOT NULL ENABLE,
	         path varchar2(500)   NOT NULL ENABLE,
                 CONSTRAINT dbsbuffer_dataset_unique UNIQUE (path)
               ) %s"""

        self.create["01dbsbuffer_dataset_seq"] = \
          """CREATE SEQUENCE dbsbuffer_dataset_seq
               start with 1
               increment by 1
               nomaxvalue"""

        self.create["01dbsbuffer_dataset_trg"] = \
          """CREATE TRIGGER dbsbuffer_dataset_trg
               BEFORE INSERT ON dbsbuffer_dataset
               FOR EACH ROW
                 BEGIN
                   SELECT dbsbuffer_dataset_seq.nextval INTO :NEW.ID FROM dual;
                 END;"""

        self.create["02dbsbuffer_algo"] = \
          """CREATE TABLE dbsbuffer_algo
               (
                 id        NUMBER(11)    NOT NULL,
                 app_name  varchar2(100) NOT NULL,
                 app_ver   varchar2(100) NOT NULL,
                 app_fam   varchar2(100) NOT NULL,
                 pset_hash varchar2(700) NOT NULL,
                 config_content CLOB,
                 in_dbs    NUMBER(11)
               ) %s"""

        self.create["02dbsbuffer_algo_seq"] = \
          """CREATE SEQUENCE dbsbuffer_algo_seq
               start with 1
               increment by 1
               nomaxvalue"""

        self.create["02dbsbuffer_algo_trg"] = \
          """CREATE TRIGGER dbsbuffer_algo_trg
               BEFORE INSERT ON dbsbuffer_algo
               FOR EACH ROW
                 BEGIN
                   SELECT dbsbuffer_algo_seq.nextval INTO :new.ID FROM dual;
                 END; """

        self.create["03dbsbuffer_algo_dataset_assoc"] = \
          """CREATE TABLE dbsbuffer_algo_dataset_assoc
               (
                 id         INTEGER NOT NULL,
                 algo_id    INTEGER NOT NULL,
                 dataset_id INTEGER NOT NULL,
                 in_dbs     INTEGER DEFAULT 0
               ) %s"""

        self.create["03dbsbuffer_algo_dataset_assoc_seq"] = \
          """CREATE SEQUENCE dbsbuffer_algdset_assoc_seq
               start with 1
               increment by 1
               nomaxvalue"""

        self.create["03dbsbuffer_algo_dataset_assoc_trg"] = \
          """CREATE TRIGGER dbsbuffer_algdset_assoc_trg
               BEFORE INSERT ON dbsbuffer_algo_dataset_assoc
               FOR EACH ROW
                BEGIN
                  SELECT dbsbuffer_algdset_assoc_seq.nextval INTO :new.id FROM dual;
                END;"""

        self.create["04dbsbuffer_file"] = \
          """CREATE TABLE dbsbuffer_file
               (
                 id                    NUMBER(11)    NOT NULL ENABLE,
                 lfn                   VARCHAR2(255) NOT NULL ENABLE,
                 filesize              NUMBER(11),
                 events                INTEGER,
                 cksum                 NUMBER(11),
                 dataset_algo          NUMBER(11)    NOT NULL ENABLE,
                 status                varchar2(20),
                 LastModificationDate  NUMBER(11)
               ) %s"""

        self.create["04dbsbuffer_file_seq"] = \
          """CREATE SEQUENCE dbsbuffer_file_seq
               start with 1
               increment by 1
               nomaxvalue"""

        self.create["04dbsbuffer_file_trg"] = \
          """CREATE TRIGGER dbsbuffer_file_trg
               BEFORE INSERT ON dbsbuffer_file
               FOR EACH ROW
                BEGIN
                  SELECT dbsbuffer_file_seq.nextval INTO :new.id FROM dual;
                END;"""

        self.create["05dbsbuffer_file_parent"] = \
          """CREATE TABLE dbsbuffer_file_parent
               (
                 child  NUMBER(11) NOT NULL,
                 parent NUMBER(11) NOT NULL
               ) %s"""

        self.create["06dbsbuffer_file_runlumi_map"] = \
          """CREATE TABLE dbsbuffer_file_runlumi_map
               (
                 filename  INTEGER NOT NULL,
                 run       INTEGER NOT NULL,
                 lumi      INTEGER NOT NULL
               ) %s"""

        self.create["07dbsbuffer_location"] = \
          """CREATE TABLE dbsbuffer_location
               (
                 id      INTEGER       NOT NULL ENABLE,
                 se_name VARCHAR2(255) NOT NULL ENABLE,
                 CONSTRAINT dbsbuffer_location_unique UNIQUE (se_name)
               ) %s"""

        self.create["07dbsbuffer_location_seq"] = \
          """CREATE SEQUENCE dbsbuffer_location_seq
               start with 1
               increment by 1
               nomaxvalue"""

        self.create["07dbsbuffer_location_trg"] = \
          """CREATE TRIGGER dbsbuffer_location_trg
             BEFORE INSERT ON dbsbuffer_location
             FOR EACH ROW
               BEGIN
                 SELECT dbsbuffer_location_seq.nextval INTO :new.id FROM dual;
               END;"""

        self.create["08dbsbuffer_file_location"] = \
          """CREATE TABLE dbsbuffer_file_location
               (
                 filename INTEGER NOT NULL,
                 location INTEGER NOT NULL
               ) %s"""

        self.indexes["1_pk_dbsbuffer_dataset"] = \
          """ALTER TABLE dbsbuffer_dataset ADD
               (CONSTRAINT pk_dbsbuffer_dataset PRIMARY KEY (id) %s)"""

        self.indexes["1_pk_dbsbuffer_algo"] = \
          """ALTER TABLE dbsbuffer_algo ADD
               (CONSTRAINT pk_dbsbuffer_algo PRIMARY KEY (id, app_name, app_ver, app_fam, pset_hash) %s)"""

        self.indexes["1_pk_dbsbuffer_algodset_assoc"] = \
          """ALTER TABLE dbsbuffer_algo_dataset_assoc ADD
               (CONSTRAINT pk_dbsbuffer_algodset_assoc PRIMARY KEY (id) %s)"""

        self.indexes["1_pk_dbsbuffer_file"] = \
          """ALTER TABLE dbsbuffer_file ADD
               (CONSTRAINT pk_dbsbuffer_file PRIMARY KEY (id) %s)"""

        self.indexes["1_pk_dbsbuffer_file_parent"] = \
          """ALTER TABLE dbsbuffer_file_parent ADD
               (CONSTRAINT pk_dbsbuffer_file_parent PRIMARY KEY (child, parent) %s)"""

        self.indexes["1_pk_dbsbuffer_file_runlumi"] = \
          """ALTER TABLE dbsbuffer_file_runlumi_map ADD
               (CONSTRAINT pk_dbsbuffer_file_runlumi PRIMARY KEY (filename) %s)"""

        self.indexes["1_pk_dbsbuffer_location"] = \
          """ALTER TABLE dbsbuffer_location ADD
               (CONSTRAINT pk_dbsbuffer_location PRIMARY KEY (id, se_name) %s)"""

        self.indexes["1_pk_dbsbuffer_file_location"] = \
          """ALTER TABLE dbsbuffer_file_location ADD
               (CONSTRAINT pk_dbsbuffer_file_location PRIMARY KEY(filename, location) %s)"""

        # If we have a tablespace for tables passed in, append it to the
        # CREATE TABLE statements.
        if params.has_key("tablespace_table"):
            tableSpaceParam = "TABLESPACE %s" % params["tablespace_table"]
        else:
            tableSpaceParam = ""
            
        for createStatement in self.create.keys():
            try:
                self.create[createStatement] = self.create[createStatement] % tableSpaceParam
            except Exception, ex:
                continue

        # If we have a tablespace parameter for indexes, apply it to our index
        # statements.
        if params.has_key("tablespace_index"):
            tableSpaceParam = "USING INDEX TABLESPACE %s" % params["tablespace_index"]
        else:
            tableSpaceParam = ""

        for indexStatement in self.indexes.keys():
            self.indexes[indexStatement] = self.indexes[indexStatement] % tableSpaceParam
