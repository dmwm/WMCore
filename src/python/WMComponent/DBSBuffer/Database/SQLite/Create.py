"""
_Create_DBSBuffer_

Implementation of Create_DBSBuffer for SQLite
"""

__revision__ = ""
__version__ = ""
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
        
        self.create["01dbsbuffer_dataset"] = \
              """CREATE TABLE dbsbuffer_dataset
			(
			   ID     INTEGER PRIMARY KEY,
			   Path   varchar(500)    unique not null,
			   UnMigratedFiles BIGINT UNSIGNED Default 0,
			   Algo bigint,
			   AlgoInDBS    int, 
			   LastModificationDate  BIGINT
			) """

        self.create["03dbsbuffer_algo"] = \
              """CREATE TABLE dbsbuffer_algo
                	(
               		   ID     INTEGER PRIMARY KEY,   
               		   AppName varchar(100),
               		   AppVer  varchar(100),
               		   AppFam  varchar(100),
               		   PSetHash varchar(700),
               		   ConfigContent LONGTEXT,
               		   LastModificationDate  BIGINT,
               		   unique (AppName,AppVer,AppFam,PSetHash) 
            		) """
                
        self.create["04dbsbuffer_file"] = \
          """CREATE TABLE dbsbuffer_file (
             id           INTEGER      PRIMARY KEY,
             lfn          VARCHAR(255) NOT NULL,
             size         BIGINT,
             events       INTEGER,
             cksum        BIGINT UNSIGNED,
	     dataset 	  BIGINT UNSIGNED   not null,
	     status       varchar(20),
             first_event  INTEGER,
             last_event   INTEGER,
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
             file    INTEGER NOT NULL,
             run     INTEGER NOT NULL,
             lumi    INTEGER NOT NULL,
             FOREIGN KEY (file) references dbsbuffer_file(id)
               ON DELETE CASCADE)"""

        self.create["07dbsbuffer_location"] = \
          """CREATE TABLE dbsbuffer_location (
             id      INTEGER      PRIMARY KEY,
             se_name VARCHAR(255) NOT NULL,
             UNIQUE(se_name))"""

        self.create["08dbsbuffer_file_location"] = \
          """CREATE TABLE dbsbuffer_file_location (
             file     INTEGER NOT NULL,
             location INTEGER NOT NULL,
             UNIQUE(file, location),
             FOREIGN KEY(file)     REFERENCES dbsbuffer_file(id)
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
                 
	#self.triggers IS NOT a member so I will just use self.create for now

        #I have no idea if this works, but it runs
        # -mnorman


        
        self.create["09TR_dbsbuffer_file_lud"]=\
                """CREATE TRIGGER TR_dbsbuffer_file_lud BEFORE INSERT ON dbsbuffer_file
                   FOR EACH ROW
                   BEGIN
                        UPDATE dbsbuffer_file
                        SET LastModificationDate = strftime('%s', 'now')
                        WHERE ID = NEW.ID;
                   END;"""

        self.create["10TR_dbsbuffer_ds_lud"]=\
                """CREATE TRIGGER TR_dbsbuffer_ds_lud BEFORE INSERT ON dbsbuffer_dataset
                     FOR EACH ROW
                     BEGIN
                        UPDATE dbsbuffer_dataset
                        SET LastModificationDate = strftime('%s', 'now')
                        WHERE ID = NEW.ID;
                     END;"""
                        
        self.create["11TR_dbsbuffer_algo_lud"]=\
                """CREATE TRIGGER TR_dbsbuffer_algo_lud BEFORE INSERT ON dbsbuffer_algo
                      FOR EACH ROW
                      BEGIN
                        UPDATE dbsbuffer_algo
                        SET LastModificationDate = strftime('%s', 'now')
                        WHERE ID = NEW.ID;
                      END;"""


