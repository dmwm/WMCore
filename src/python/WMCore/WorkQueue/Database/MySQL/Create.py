"""
_CreateWorkQueue_

Implementation of CreateWorkQueue for MySQL.

Inherit from CreateWorkQueueBase, and add MySQL specific substitutions (e.g. add 
INNODB).
"""




from WMCore.WorkQueue.Database.CreateWorkQueueBase import CreateWorkQueueBase

class Create(CreateWorkQueueBase):
    """
    Class to set up the WorkQueue schema in a MySQL database
    """
    def __init__(self, logger = None, dbi = None, params = None):
        """
        _init_

        Call the base class's constructor and create all necessary tables,
        constraints and inserts.
        """
        CreateWorkQueueBase.__init__(self, logger, dbi, params)

        # overwrite some tables to use MySql auto increment feature
        self.create["01wq_wmspec"] = \
          """CREATE TABLE wq_wmspec (
             id          INTEGER      NOT NULL AUTO_INCREMENT, 
             name        VARCHAR(255) NOT NULL,
             url         VARCHAR(255) NOT NULL,
             owner       VARCHAR(255) NOT NULL,
             PRIMARY KEY(id),
             UNIQUE (url),
             UNIQUE (name)
             )"""
        
        self.create["02wq_wmtask"] = \
          """CREATE TABLE wq_wmtask (
             id          INTEGER      NOT NULL AUTO_INCREMENT,
             wmspec_id  INTEGER NOT NULL,
             name         VARCHAR(500) NOT NULL,
             type       VARCHAR(255) NOT NULL,
             dbs_url    VARCHAR(500),
             PRIMARY KEY(id),
             UNIQUE (wmspec_id, name)
             )"""
             
        self.create["03wq_data"] = \
          """CREATE TABLE wq_data (
             id             INTEGER      NOT NULL AUTO_INCREMENT,
             name           VARCHAR(500) NOT NULL,
             PRIMARY KEY(id),
             UNIQUE (name)
             )"""

        self.create["04wq_queues"] = \
          """CREATE TABLE wq_queues (
             id               INTEGER    NOT NULL AUTO_INCREMENT,
             url              VARCHAR(500) NOT NULL,
             PRIMARY KEY (id),
             UNIQUE(url)
             )"""
             

        self.create["05wq_element"] = \
          """CREATE TABLE wq_element (
             id               INTEGER    NOT NULL AUTO_INCREMENT,
             request_name     VARCHAR(255),
             wmtask_id        INTEGER    NOT NULL,
             input_id         INTEGER,
             parent_queue_id  INTEGER,
             child_queue      INTEGER,
             num_jobs         INTEGER    NOT NULL,
             priority         INTEGER    NOT NULL,
             parent_flag      INTEGER    DEFAULT 0,
             status           INTEGER    DEFAULT 0,
             subscription_id  INTEGER    NOT NULL,
             team_name        VARCHAR(255),
             events_written   INTEGER    DEFAULT 0,
             files_processed  INTEGER    DEFAULT 0,
             percent_complete INTEGER    DEFAULT 0,
             percent_success  INTEGER    DEFAULT 0,
             insert_time      INTEGER    NOT NULL,
             update_time      INTEGER    NOT NULL,
             reqmgr_time      INTEGER    DEFAULT 0,
             PRIMARY KEY (id)
             ) """

        self.create["07wq_site"] = \
              """CREATE TABLE wq_site (
                 id          INTEGER      NOT NULL AUTO_INCREMENT,
                 name        VARCHAR(255) NOT NULL,
                 PRIMARY KEY(id),
                 UNIQUE(name)
                 )"""

    def execute(self, conn = None, transaction = None):
        for i in self.create.keys():
            self.create[i] = self.create[i] + " ENGINE=InnoDB"
            #self.create[i] = self.create[i].replace('INTEGER', 'INT(11)')

        return CreateWorkQueueBase.execute(self, conn, transaction)
