"""
_Create_DBSBuffer_

Implementation of Create_DBSBuffer for SQLite
"""

__revision__ = "$Id: Create.py,v 1.3 2009/07/13 19:17:13 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

import logging
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
                  id   INTEGER PRIMARY KEY AUTOINCREMENT,
                  path VARCHAR(500) UNIQUE NOT NULL
               )"""

        self.create["02dbsbuffer_algo"] = \
          """CREATE TABLE dbsbuffer_algo
               (
                  id             INTEGER PRIMARY KEY AUTOINCREMENT,   
                  app_name       VARCHAR(100),
                  app_ver        VARCHAR(100),
                  app_fam        VARCHAR(100),
                  pset_hash      VARCHAR(700),
                  config_content LONGTEXT,
                  in_dbs         INTEGER DEFAULT 0,
                  UNIQUE(app_name, app_ver, app_fam, pset_hash) 
               )"""

        self.create["03dbsbuffer_algo_dataset_assoc"] = \
          """CREATE TABLE dbsbuffer_algo_dataset_assoc
               (
                  id               INTEGER PRIMARY KEY AUTOINCREMENT,
                  algo_id          BIGINT UNSIGNED,
                  dataset_id       BIGINT UNSINGED,
                  unmigrated_files BIGINT UNSIGNED DEFAULT 0,
                  in_dbs           INTEGER DEFAULT 0,
                  FOREIGN KEY (algo_id) REFERENCES dbsbuffer_algo(id)
                    ON DELETE CASCADE,
                  FOREIGN KEY (dataset_id) REFERENCES dbsbuffer_algo(id)
                    ON DELETE CASCADE
               )"""
                
        self.create["04dbsbuffer_file"] = \
          """CREATE TABLE dbsbuffer_file (
             id           INTEGER      PRIMARY KEY AUTOINCREMENT,
             lfn          VARCHAR(255) NOT NULL,
             filesize     BIGINT,
             events       INTEGER,
             cksum        BIGINT UNSIGNED,
             dataset_algo INTEGER NOT NULL,
	     status       varchar(20),
             LastModificationDate  BIGINT)"""
        
        self.create["05dbsbuffer_file_parent"] = \
          """CREATE TABLE dbsbuffer_file_parent (
             child  INTEGER NOT NULL,
             parent INTEGER NOT NULL,
             FOREIGN KEY (child)  references dbsbuffer_file(id)
               ON DELETE CASCADE,
             FOREIGN KEY (parent) references dbsbuffer_file(id),
             UNIQUE(child, parent))"""

        self.create["06dbsbuffer_file_runlumi_map"] = \
          """CREATE TABLE dbsbuffer_file_runlumi_map (
             filename    INTEGER NOT NULL,
             run         INTEGER NOT NULL,
             lumi        INTEGER NOT NULL,
             FOREIGN KEY (filename) references dbsbuffer_file(id)
               ON DELETE CASCADE)"""

        self.create["07dbsbuffer_location"] = \
          """CREATE TABLE dbsbuffer_location (
             id      INTEGER      PRIMARY KEY,
             se_name VARCHAR(255) NOT NULL,
             UNIQUE(se_name))"""

        self.create["08dbsbuffer_file_location"] = \
          """CREATE TABLE dbsbuffer_file_location (
             filename INTEGER NOT NULL,
             location INTEGER NOT NULL,
             UNIQUE(filename, location),
             FOREIGN KEY(filename) REFERENCES dbsbuffer_file(id)
               ON DELETE CASCADE,
             FOREIGN KEY(location) REFERENCES dbsbuffer_location(id)
               ON DELETE CASCADE)"""

        self.constraints["FK_dbsbuffer_file_ds"]=\
		      """ALTER TABLE dbsbuffer_file ADD CONSTRAINT FK_dbsbuffer_file_ds
    			 foreign key(Dataset) references dbsbuffer_dataset(ID) on delete CASCADE"""

        self.constraints["FK_dbsbuffer_file_ds"]=\
                      """ALTER TABLE dbsbuffer_file ADD CONSTRAINT FK_dbsbuffer_dbsbuffile
                         foreign key(WMBS_File_ID) references dbsbuffer_file(ID)"""

        self.constraints["FK_dbsbuffer_ds_algo"]=\
              """ALTER TABLE dbsbuffer_algo DD CONSTRAINT FK_dbsbuffer_ds_algo
                 foreign key(Algo) references dbsbuffer_algo(ID)"""
