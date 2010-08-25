"""
_CreateWorkQueue_

Implementation of CreateWorkQueue for MySQL.

Inherit from CreateWMBSBase, and add MySQL specific substitutions (e.g. add 
INNODB) and specific creates (e.g. for time stamp and enum fields).
"""

__revision__ = "$Id: Create.py,v 1.4 2009/06/19 22:15:16 sryu Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WorkQueue.Database.CreateWorkQueueBase import CreateWorkQueueBase

class Create(CreateWorkQueueBase):
    """
    Class to set up the WMBS schema in a MySQL database
    """
    def __init__(self, logger = None, dbi = None):
        """
        _init_

        Call the base class's constructor and create all necessary tables,
        constraints and inserts.
        """        
        CreateWorkQueueBase.__init__(self, logger, dbi)
        
        # overwrite some tables to use MySql auto increment feature
        self.create["01wq_wmspec"] = \
          """CREATE TABLE wq_wmspec (
             id          INTEGER      NOT NULL AUTO_INCREMENT, 
             name        VARCHAR(255) NOT NULL,
             PRIMARY KEY(id))"""
                                    
        self.create["02wq_block"] = \
          """CREATE TABLE wq_block (
             id             INTEGER      NOT NULL AUTO_INCREMENT,
             name           VARCHAR(500) NOT NULL,
             block_size     INTEGER      NOT NULL,
             num_files      INTEGER      NOT NULL,
             num_event      INTEGER      NOT NULL,
             PRIMARY KEY(id)
             )"""
             
        self.create["04wq_element"] = \
          """CREATE TABLE wq_element (
             id               INTEGER    NOT NULL AUTO_INCREMENT,
             wmspec_id        INTEGER    NOT NULL,
             block_id         INTEGER    NOT NULL,
             num_jobs         INTEGER    NOT NULL,
             priority         INTEGER    NOT NULL,
             parent_flag      INTEGER    DEFAULT 0,
             status           INTEGER    DEFAULT 0,
             insert_time      INTEGER    NOT NULL,
             PRIMARY KEY (id),
             UNIQUE (wmspec_id, block_id)
             ) """

        
    def execute(self, conn = None, transaction = None):
        for i in self.create.keys():
            self.create[i] = self.create[i] + " ENGINE=InnoDB"
            #self.create[i] = self.create[i].replace('INTEGER', 'INT(11)')
            
        return CreateWorkQueueBase.execute(self, conn, transaction)
        
