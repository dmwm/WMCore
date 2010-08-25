"""
_Create_DBSBuffer_

Implementation of Create_DBSBuffer for MySQL.
"""

__revision__ = "$Id: Create.py,v 1.17 2009/05/18 20:13:28 mnorman Exp $"
__version__ = "$Revision: 1.17 $"
__author__ = "anzar@fnal.gov"

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
			   ID     BIGINT UNSIGNED not null auto_increment,
			   Path   varchar(500)    unique not null,
			   UnMigratedFiles BIGINT UNSIGNED Default 0,
			   Algo bigint,
			   AlgoInDBS    int, 
			   LastModificationDate  BIGINT,
			   primary key(ID)	
			) ENGINE=InnoDB"""

        self.create["03dbsbuffer_algo"] = \
              """CREATE TABLE dbsbuffer_algo
                	(
               		   ID     BIGINT UNSIGNED not null auto_increment,   
               		   AppName varchar(100),
               		   AppVer  varchar(100),
               		   AppFam  varchar(100),
               		   PSetHash varchar(700),
               		   ConfigContent LONGTEXT,
               		   LastModificationDate  BIGINT,
               		   primary key(ID),
               		   unique (AppName,AppVer,AppFam,PSetHash) 
            		) ENGINE=InnoDB"""
                
        self.create["04dbsbuffer_file"] = \
          """CREATE TABLE dbsbuffer_file (
             id           INTEGER      PRIMARY KEY AUTO_INCREMENT,
             lfn          VARCHAR(255) NOT NULL,
             filesize     BIGINT,
             events       INTEGER,
             cksum        BIGINT UNSIGNED,
	     dataset 	  BIGINT UNSIGNED   not null,
	     status       varchar(20),
             first_event  INTEGER,
             last_event   INTEGER,
             LastModificationDate  BIGINT)ENGINE=InnoDB"""
        
        self.create["05dbsbuffer_file_parent"] = \
          """CREATE TABLE dbsbuffer_file_parent (
             child  INTEGER NOT NULL,
             parent INTEGER NOT NULL,
             FOREIGN KEY (child)  references dbsbuffer_file(id)
               ON DELETE CASCADE,
             FOREIGN KEY (parent) references dbsbuffer_file(id),
             UNIQUE(child, parent))ENGINE=InnoDB"""

        self.create["06dbsbuffer_file_runlumi_map"] = \
          """CREATE TABLE dbsbuffer_file_runlumi_map (
             filename    INTEGER NOT NULL,
             run         INTEGER NOT NULL,
             lumi        INTEGER NOT NULL,
             FOREIGN KEY (file) references dbsbuffer_file(id)
               ON DELETE CASCADE)ENGINE=InnoDB"""

        self.create["07dbsbuffer_location"] = \
          """CREATE TABLE dbsbuffer_location (
             id      INTEGER      PRIMARY KEY AUTO_INCREMENT,
             se_name VARCHAR(255) NOT NULL,
             UNIQUE(se_name))ENGINE=InnoDB"""

        self.create["08dbsbuffer_file_location"] = \
          """CREATE TABLE dbsbuffer_file_location (
             filename INTEGER NOT NULL,
             location INTEGER NOT NULL,
             UNIQUE(file, location),
             FOREIGN KEY(filename) REFERENCES dbsbuffer_file(id)
               ON DELETE CASCADE,
             FOREIGN KEY(location) REFERENCES dbsbuffer_location(id)
               ON DELETE CASCADE)ENGINE=InnoDB"""

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
                        FOR EACH ROW SET NEW.LastModificationDate = UNIX_TIMESTAMP();"""

        self.create["10TR_dbsbuffer_ds_lud"]=\
                """CREATE TRIGGER TR_dbsbuffer_ds_lud BEFORE INSERT ON dbsbuffer_dataset
                        FOR EACH ROW SET NEW.LastModificationDate = UNIX_TIMESTAMP();"""
                        
        self.create["11TR_dbsbuffer_algo_lud"]=\
                """CREATE TRIGGER TR_dbsbuffer_algo_lud BEFORE INSERT ON dbsbuffer_algo
                        FOR EACH ROW SET NEW.LastModificationDate = UNIX_TIMESTAMP();"""

