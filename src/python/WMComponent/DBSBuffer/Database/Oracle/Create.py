"""
_Create_DBSBuffer_

Implementation of Create_DBSBuffer for Oracle.
"""

__revision__ = "$Id: Create.py,v 1.7 2009/08/13 22:57:12 meloam Exp $"
__version__ = "$Revision: 1.7 $"

import threading

from WMCore.Database.DBCreator import DBCreator

class Create(DBCreator):

    def __init__(self, logger = None, dbi = None, params = None):
        """
        _init_

        Call the base class's constructor and create all necessary tables,
        constraints and inserts.
        """
        myThread = threading.currentThread()
        DBCreator.__init__(self, myThread.logger, myThread.dbi)

        if params == None:
            params = {}
            params["tablespace_table"] = ''
            params["tablespace_index"] = ''

        self.create["01dbsbuffer_dataset"] = \
          """CREATE TABLE dbsbuffer_dataset
               (
	         id   NUMBER(11)      NOT NULL ENABLE,
	         path varchar2(500)   NOT NULL ENABLE
               )%s""" %(params["tablespace_table"])

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
                 id        NUMBER(11)     NOT NULL ENABLE,   
                 app_name  varchar2(100),
                 app_ver   varchar2(100),
                 app_fam   varchar2(100),
                 pset_hash varchar2(700),
                 config_content CLOB,
                 in_dbs    NUMBER(11)
               )%s""" %(params["tablespace_table"])

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
               )%s""" %(params["tablespace_table"])

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
               )%s""" %(params["tablespace_table"])

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
               )%s""" %(params["tablespace_table"])

        self.create["06dbsbuffer_file_runlumi_map"] = \
          """CREATE TABLE dbsbuffer_file_runlumi_map
               (
                 filename  INTEGER NOT NULL,
                 run       INTEGER    NOT NULL ENABLE,
                 lumi      INTEGER    NOT NULL ENABLE
               )%s""" %(params["tablespace_table"])

        self.create["07dbsbuffer_location"] = \
          """CREATE TABLE dbsbuffer_location
               (
                 id      INTEGER       NOT NULL ENABLE,
                 se_name VARCHAR2(255) NOT NULL ENABLE,
                 CONSTRAINT dbsbuffer_location_unique UNIQUE (se_name),
                 CONSTRAINT dbsbuffer_location_pk     PRIMARY KEY (id)
               )%s""" %(params["tablespace_table"])

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
                 filename   INTEGER NOT NULL,
                 location   INTEGER NOT NULL
               )%s""" %(params["tablespace_table"])

        self.create["10dbsbuffer_block"] = \
          """CREATE TABLE dbsbuffer_block (
          id        INTEGER      PRIMARY KEY AUTO_INCREMENT,
          blockname VARCHAR(250) NOT NULL,
          location  INTEGER      NOT NULL,
          is_in_phedex INTEGER NOT NULL DEFAULT 0,
          CONSTRAINT dbsbuffer_block_pk     PRIMARY KEY (id),
          CONSTRAINT dbsbuffer_block_unique UNIQUE (blockname, location)"""

        self.create["10dbsbuffer_block_seq"] = \
          """CREATE SEQUENCE dbsbuffer_block_seq
          start with 1
          increment by 1
          nomaxvalue"""

        self.create["10dbsbuffer_block_trg"] = \
          """CREATE TRIGGER dbsbuffer_block_trg
          BEFORE INSERT ON dbsbuffer_block
          FOR EACH ROW
          BEGIN
            SELECT dbsbuffer_block_seq.nextval INTO :new.id FROM dual;
          END;"""       


        self.indexes["1_pk_dbsbuffer_dataset"] = \
          """ALTER TABLE dbsbuffer_dataset ADD
               (CONSTRAINT dbsbuffer_dataset_pk     PRIMARY KEY (id) %s),
               (CONSTRAINT dbsbuffer_dataset_unique UNIQUE (Path) %s)""" %(params["tablespace_index"], params["tablespace_index"])
        
        self.indexes["1_pk_dbsbuffer_algo"] = \
          """ALTER TABLE dbsbuffer_algo ADD
               (CONSTRAINT dbsbuffer_algo_pk     PRIMARY KEY (id) %s),
               (CONSTRAINT dbsbuffer_algo_unique UNIQUE      (app_name, app_ver, app_fam, pset_hash) %s)
               """ %(params["tablespace_index"], params["tablespace_index"])
        
        
        self.indexes["1_pk_dbsbuffer_file"] = \
          """ALTER TABLE dbsbuffer_file ADD
               (CONSTRAINT dbsbuffer_file_pk PRIMARY KEY (id) %s)""" %(params["tablespace_index"])
        
        self.indexes["1_pk_dbsbuffer_file_parent"] = \
          """ALTER TABLE dbsbuffer_file_parent ADD
               (CONSTRAINT dbsbuffer_file_parent_child  FOREIGN KEY (child)  REFERENCES dbsbuffer_file(id)
                 ON DELETE CASCADE)
               (CONSTRAINT dbsbuffer_file_parent_parent FOREIGN KEY (parent) REFERENCES dbsbuffer_file(id)
                 ON DELETE CASCADE)"""
        
        self.indexes["1_pk_dbsbuffer_location"] = \
          """ALTER TABLE dbsbuffer_location ADD
               (CONSTRAINT pk_dbsbuffer_location_pk     PRIMARY KEY (id) %s)
               (CONSTRAINT pk_dbsbuffer_location_unique UNIQUE (se_name) %s)""" %(params["tablespace_index"], params["tablespace_index"])

        #FOREIGN KEYS sometimes have to be done after the uniqueness of their target is established
        #Do them down here.


        self.indexes["2_pk_dbsbuffer_algodset_assoc"] = \
          """ALTER TABLE dbsbuffer_algo_dataset_assoc ADD
               (CONSTRAINT dbsbuffer_algodset_assoc_pk PRIMARY KEY (id) %s),
               (CONSTRAINT dbsbuffer_algodset_assoc_ds FOREIGN KEY (dataset_id) REFERENCES dbsbuffer_dataset(id)
                 ON DELETE CASCADE),
               (CONSTRAINT dbsbuffer_algodset_assoc_al FOREIGN KEY (algo_id)    REFERENCES dbsbuffer_algo(id)
                 ON DELETE CASCADE)""" %(params["tablespace_index"])

        self.indexes["2_pk_dbsbuffer_file_runlumi"] = \
          """ALTER TABLE dbsbuffer_file_runlumi_map ADD
               (CONSTRAINT dbsbuffer_file_runlumi_pk FOREIGN KEY (filename) REFERENCES dbsbuffer_file(id)
                 ON DELETE CASCADE)""" 
        
        self.indexes["2_pk_dbsbuffer_file_location"] = \
          """ALTER TABLE dbsbuffer_file_location ADD
               (CONSTRAINT dbsbuffer_file_location_loc  FOREIGN KEY (location) REFERENCES dbsbuffer_location(id)
                 ON DELETE CASCADE)
               (CONSTRAINT dbsbuffer_file_location_file FOREIGN KEY (filename) REFERENCES dbsbuffer_file(id)
                 ON DELETE CASCADE)""" 
