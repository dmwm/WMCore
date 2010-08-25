"""
_CreateWorkQueue_

Implementation of CreateWorkQueue for MySQL.

Inherit from CreateWorkQueueBase, and add MySQL specific substitutions (e.g. add 
INNODB).
"""

__revision__ = "$Id: Create.py,v 1.14 2009/11/12 16:43:31 swakef Exp $"
__version__ = "$Revision: 1.14 $"

from WMCore.WorkQueue.Database.CreateWorkQueueBase import CreateWorkQueueBase

class Create(CreateWorkQueueBase):
    """
    Class to set up the WorkQueue schema in a MySQL database
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
             name        VARCHAR(500) NOT NULL,
             url         VARCHAR(500) NOT NULL,
             PRIMARY KEY(id),
             UNIQUE (name)
             )"""

        self.create["02wq_data"] = \
          """CREATE TABLE wq_data (
             id             INTEGER      NOT NULL AUTO_INCREMENT,
             name           VARCHAR(500) NOT NULL,
             PRIMARY KEY(id),
             UNIQUE (name)
             )"""

        self.create["03wq_queues"] = \
          """CREATE TABE wq_queues (
             id               INTEGER    NOT NULL AUTO_INCREMENT,
             url              VARCHAR(255) NOT NULL,
             PRIMARY KEY (id, url),
             UNIQUE(url)"""

        self.create["04wq_element"] = \
          """CREATE TABLE wq_element (
             id               INTEGER    NOT NULL AUTO_INCREMENT,
             wmspec_id        INTEGER    NOT NULL,
             input_id         INTEGER,
             parent_queue_id  INTEGER,
             child_queue      VARCHAR(255),
             num_jobs         INTEGER    NOT NULL,
             priority         INTEGER    NOT NULL,
             parent_flag      INTEGER    DEFAULT 0,
             status           INTEGER    DEFAULT 0,
             subscription_id  INTEGER    NOT NULL,
             insert_time      INTEGER    NOT NULL,
             update_time      INTEGER    NOT NULL,
             PRIMARY KEY (id),
             UNIQUE (wmspec_id, subscription_id)
             ) """

        self.create["06wq_site"] = \
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
