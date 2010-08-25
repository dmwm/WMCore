"""
_CreateWorkQueue_

Implementation of CreateWorkQueue for SQLite.

"""

__revision__ = "$Id: Create.py,v 1.10 2010/01/25 19:29:45 sryu Exp $"
__version__ = "$Revision: 1.10 $"

from WMCore.WorkQueue.Database.CreateWorkQueueBase import CreateWorkQueueBase

class Create(CreateWorkQueueBase):
    """
    Class to set up the WMBS schema in a MySQL database
    """
    def __init__(self, logger = None, dbi = None, params = None):
        """
        _init_

        Call the base class's constructor and create all necessary tables,
        constraints and inserts.
        """
        CreateWorkQueueBase.__init__(self, logger, dbi, params)

        # Can't use ALTER TABLE to add constrints
        self.create["05wq_element"] = \
          """CREATE TABLE wq_element (
             id               INTEGER    NOT NULL,
             wmtask_id        INTEGER    NOT NULL REFERENCES wq_wmspec(id),
             input_id         INTEGER             REFERENCES wq_data(id),
             parent_queue_id  INTEGER,
             child_queue      VARCHAR(255)        REFERENCES wq_queues(id),
             num_jobs         INTEGER    NOT NULL,
             priority         INTEGER    NOT NULL,
             parent_flag      INTEGER    DEFAULT 0,
             status           INTEGER    DEFAULT 0 REFERENCES wq_element_status(id),
             subscription_id  INTEGER,
             insert_time      INTEGER    NOT NULL,
             update_time      INTEGER    NOY NULL,
             PRIMARY KEY (id)
             ) """

        self.create["06wq_data_parentage"] = \
          """CREATE TABLE wq_data_parentage (
             child        INTEGER    NOT NULL REFERENCES wq_data(id),
             parent       INTEGER    NOT NULL REFERENCES wq_data(id),
             PRIMARY KEY (child, parent)
             )"""

        self.create["08wq_data_site_assoc"] = \
          """CREATE TABLE wq_data_site_assoc (
             data_id     INTEGER    NOT NULL REFERENCES wq_data(id),
             site_id      INTEGER    NOT NULL REFERENCES wq_site(id),
             -- online BOOL DEFAULT FALSE, -- for when we track staging
             PRIMARY KEY (data_id, site_id)
             )""" #-- PRIMARY KEY (block_id, site_id) #-- online BOOL DEFAULT FALSE, -- for when we track staging

        self.create["09wq_element_site_validation"] = \
          """CREATE TABLE wq_element_site_validation (
             element_id   INTEGER    NOT NULL REFERENCES wq_element(id),
             site_id      INTEGER    NOT NULL REFERENCES wq_site(id),
             valid    CHAR(1) CHECK(valid IN (0, 1)),
             PRIMARY KEY (element_id, site_id)
             )"""
        # constraints added in table definition
        self.constraints.clear()
