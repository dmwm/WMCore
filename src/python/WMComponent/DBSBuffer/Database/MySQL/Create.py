"""
_Create_DBSBuffer_

Implementation of Create_DBSBuffer for MySQL.
"""

__revision__ = "$Id: Create.py,v 1.12 2008/11/11 19:47:48 afaq Exp $"
__version__ = "$Revision: 1.12 $"
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
        
        self.create["05dbsbuf_algo"] = \
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
                
        self.create["01dbsbuf_dataset"] = \
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

        self.create["02dbsbuf_file"] = \
		      """CREATE TABLE dbsbuffer_file
			( 
			    ID                    BIGINT UNSIGNED not null auto_increment,
			    LFN                   varchar(500)      unique not null,
			    Dataset 		      BIGINT UNSIGNED   not null,
			    Checksum              varchar(100)      not null,
			    NumberOfEvents        BIGINT UNSIGNED   not null,
			    FileSize              BIGINT UNSIGNED   not null,
			    FileStatus            varchar(20),
			    FileType              BIGINT UNSIGNED,
			    RunLumiInfo           varchar(500),
			    SEName                varchar(100),
			    LastModificationDate  BIGINT,
			    primary key(ID)
		    ) ENGINE=InnoDB"""

        self.constraints["FK_dbsbuf_file_ds"]=\
		      """ALTER TABLE dbsbuffer_file ADD CONSTRAINT FK_dbsbuf_file_ds
    			 foreign key(Dataset) references dbsbuffer_dataset(ID) on delete CASCADE"""

        self.constraints["FK_dbsbuf_ds_algo"]=\
              """ALTER TABLE dbsbuffer_algo DD CONSTRAINT FK_dbsbuf_ds_algo
                 foreign key(Algo) references dbsbuffer_algo(ID)"""
                 
	#self.triggers IS NOT a member so I will just use self.create for now
        self.create["03TR_dbsbuf_file_lud"]=\
                """CREATE TRIGGER TR_dbsbuf_file_lud BEFORE INSERT ON dbsbuffer_file
                        FOR EACH ROW SET NEW.LastModificationDate = UNIX_TIMESTAMP();"""

        self.create["04TR_dbsbuf_ds_lud"]=\
                """CREATE TRIGGER TR_dbsbuf_ds_lud BEFORE INSERT ON dbsbuffer_dataset
                        FOR EACH ROW SET NEW.LastModificationDate = UNIX_TIMESTAMP();"""
                        
        self.create["06TR_dbsbuf_algo_lud"]=\
                """CREATE TRIGGER TR_dbsbuf_algo_lud BEFORE INSERT ON dbsbuffer_algo
                        FOR EACH ROW SET NEW.LastModificationDate = UNIX_TIMESTAMP();"""

	#self.create["05TR_UnMigratedFiles"]=\
	#	"""CREATE TRIGGER TR_UnMigratedFiles AFTER INSERT ON dbsbuffer_file
	#		FOR EACH ROW 
	#		UPDATE dbsbuffer_dataset SET dbsbuffer_dataset.UnMigratedFiles = dbsbuffer_dataset.UnMigratedFiles + 1 WHERE dbsbuffer_dataset.ID = NEW.Dataset;"""


        


