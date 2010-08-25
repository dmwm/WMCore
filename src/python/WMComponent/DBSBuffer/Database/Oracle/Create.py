"""
_Create_DBSBuffer_

Implementation of Create_DBSBuffer for Oracle.
"""

__revision__ = "$Id: Create.py,v 1.4 2009/07/15 20:42:33 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

import threading

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

        self.create["01dbsbuffer_dataset"] = \
          """CREATE TABLE dbsbuffer_dataset
               (
	         id   NUMBER(11)      NOT NULL ENABLE,
	         path varchar2(500)   NOT NULL ENABLE,
                 CONSTRAINT dbsbuffer_dataset_unique UNIQUE (Path),
                 CONSTRAINT dbsbuffer_dataset_pk     PRIMARY KEY (ID)	
               )"""

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
                 in_dbs    NUMBER(11),
                 CONSTRAINT dbsbuffer_algo_unique UNIQUE (app_name, app_ver, app_fam, pset_hash),
                 CONSTRAINT dbsbuffer_algo_pk     PRIMARY KEY (ID)
               )"""

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
                 in_dbs     INTEGER DEFAULT 0,
                 FOREIGN KEY (algo_id) REFERENCES dbsbuffer_algo(id)
                   ON DELETE CASCADE,
                 FOREIGN KEY (dataset_id) REFERENCES dbsbuffer_dataset(id)
                   ON DELETE CASCADE
               )"""

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
                 LastModificationDate  NUMBER(11),
                 CONSTRAINT dbsbuffer_file_pk     PRIMARY KEY (id)
               )"""

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
               )"""

        self.create["06dbsbuffer_file_runlumi_map"] = \
          """CREATE TABLE dbsbuffer_file_runlumi_map
               (
                 filename  INTEGER NOT NULL,
                 run       INTEGER    NOT NULL ENABLE,
                 lumi      INTEGER    NOT NULL ENABLE
               )"""

        self.create["07dbsbuffer_location"] = \
          """CREATE TABLE dbsbuffer_location
               (
                 id      INTEGER       NOT NULL ENABLE,
                 se_name VARCHAR2(255) NOT NULL ENABLE,
                 CONSTRAINT dbsbuffer_location_unique UNIQUE (se_name),
                 CONSTRAINT dbsbuffer_location_pk     PRIMARY KEY (id)
               )"""

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
               )"""   
