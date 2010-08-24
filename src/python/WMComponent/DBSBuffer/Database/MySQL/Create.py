"""
_Create_DBSBuffer_

Implementation of Create_DBSBuffer for MySQL.
"""

__revision__ = "$Id: Create.py,v 1.2 2008/10/10 21:41:00 afaq Exp $"
__version__ = "$Reivison: $"
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

	self.create["TR_dbsbuf_file_lud"]=\
                """CREATE TRIGGER TR_dbsbuf_file_lud BEFORE INSERT ON dbsbuf_file
                        FOR EACH ROW SET NEW.LastModificationDate = UNIX_TIMESTAMP();"""

        self.create["TR_dbsbuf_ds_lud"]=\
                """CREATE TRIGGER TR_dbsbuf_ds_lud BEFORE INSERT ON dbsbuf_dataset
                        FOR EACH ROW SET NEW.LastModificationDate = UNIX_TIMESTAMP();"""

	return

	self.create["dbsbuf_dataset"] = \
                """CREATE TABLE dbsbuffer_dataset
			(
			   ID     BIGINT UNSIGNED not null auto_increment,
			   Path   varchar(500)    unique not null,
			   LastModificationDate  BIGINT,
			   primary key(ID)	
			) ENGINE=InnoDB"""

        self.create["dbsbuf_file"] = \
		"""CREATE TABLE dbsbuffer_file
			( 
			    ID                    BIGINT UNSIGNED not null auto_increment,
			    LFN                   varchar(500)      unique not null,
			    Dataset 		  BIGINT UNSIGNED   not null,
			    BlockName             varchar(500)      not null,
			    Checksum              varchar(100)      not null,
			    NumberOfEvents        BIGINT UNSIGNED   not null,
			    FileSize              BIGINT UNSIGNED   not null,
			    FileStatus            BIGINT UNSIGNED,
			    FileType              BIGINT UNSIGNED,
			    RunLumiInfo           varchar(500),
			    LastModificationDate  BIGINT,
			    primary key(ID)
		) ENGINE=InnoDB"""

	self.constraints["FK_dbsbuf_file_ds"]=\
		"""ALTER TABLE dbsbuf_file ADD CONSTRAINT FK_dbsbuf_file_ds
    			 foreign key(Dataset) references dbsbuffer_dataset(ID) on delete CASCADE"""

	#self.triggers IS NOT a member so I will just use self.create for now
	
	#self.triggers["TR_dbsbuf_file_lud"]=\
	#	"""CREATE TRIGGER TR_dbsbuf_file_lud BEFORE INSERT ON dbsbuf_file
	#		FOR EACH ROW SET NEW.LastModificationDate = UNIX_TIMESTAMP();"""

        #self.triggers["TR_dbsbuf_ds_lud"]=\
	#	"""CREATE TRIGGER TR_dbsbuf_ds_lud BEFORE INSERT ON dbsbuf_dataset
	#		FOR EACH ROW SET NEW.LastModificationDate = UNIX_TIMESTAMP();"""
	
	self.create["TR_dbsbuf_file_lud"]=\
                """CREATE TRIGGER TR_dbsbuf_file_lud BEFORE INSERT ON dbsbuf_file
                        FOR EACH ROW SET NEW.LastModificationDate = UNIX_TIMESTAMP();"""

        self.create["TR_dbsbuf_ds_lud"]=\
                """CREATE TRIGGER TR_dbsbuf_ds_lud BEFORE INSERT ON dbsbuf_dataset
                        FOR EACH ROW SET NEW.LastModificationDate = UNIX_TIMESTAMP();"""


        


