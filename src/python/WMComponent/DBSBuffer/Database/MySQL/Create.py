"""
_Create_DBSBuffer_

Implementation of Create_DBSBuffer for MySQL.
"""

__revision__ = "$Id: Create.py,v 1.1 2008/10/02 19:57:13 afaq Exp $"
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

        self.create["dbsbuffer_file"] = \
		"""CREATE TABLE Files
			( 
			    ID                    BIGINT UNSIGNED not null auto_increment,
			    LFN                   varchar(500)      unique not null,
			    Path                  varchar(500)      not null,
			    BlockName             varchar(500)      not null,
			    Checksum              varchar(100)      not null,
			    NumberOfEvents        BIGINT UNSIGNED   not null,
			    FileSize              BIGINT UNSIGNED   not null,
			    FileStatus            BIGINT UNSIGNED   not null,
			    FileType              BIGINT UNSIGNED   not null,
			    FileBranch            BIGINT UNSIGNED,
			    ValidationStatus      BIGINT UNSIGNED,
			    QueryableMetadata     varchar(1000)     default 'NOTSET',
			    AutoCrossSection      float,
			    CreatedBy             BIGINT UNSIGNED,  
			    CreationDate          BIGINT,
			    LastModifiedBy        BIGINT UNSIGNED,
			    LastModificationDate  BIGINT,
			    primary key(ID)
		) ENGINE=InnoDB"""


