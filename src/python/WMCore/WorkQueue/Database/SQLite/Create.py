"""
_CreateWorkQueue_

Implementation of CreateWorkQueue for SQLite.

"""

__revision__ = "$Id: Create.py,v 1.6 2009/09/07 14:41:39 swakef Exp $"
__version__ = "$Revision: 1.6 $"

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

        # Can't use ALTER TABLE to add constrints
        self.create["03wq_element"] = \
          """CREATE TABLE wq_element (
             id               INTEGER    NOT NULL,
             wmspec_id        INTEGER    NOT NULL REFERENCES wq_wmspec(id),
             input_id         INTEGER             REFERENCES wq_data(id),
             parent_queue_id  INTEGER,
             num_jobs         INTEGER    NOT NULL,
             priority         INTEGER    NOT NULL,
             parent_flag      INTEGER    DEFAULT 0,
             status           INTEGER    DEFAULT 0 REFERENCES wq_element_status(id),
             subscription_id  INTEGER    NOT NULL REFERENCES wmbs_subscription(id),
             insert_time      INTEGER    NOT NULL,
             PRIMARY KEY (id),
             UNIQUE (wmspec_id, input_id)
             ) """

        self.create["04wq_data_parentage"] = \
          """CREATE TABLE wq_data_parentage (
             child        INTEGER    NOT NULL REFERENCES wq_data(id),
             parent       INTEGER    NOT NULL REFERENCES wq_data(id),
             PRIMARY KEY (child, parent)
             )"""

        self.create["06wq_data_site_assoc"] = \
          """CREATE TABLE wq_data_site_assoc (
             data_id     INTEGER    NOT NULL REFERENCES wq_data(id),
             site_id      INTEGER    NOT NULL REFERENCES wq_site(id),
             -- online BOOL DEFAULT FALSE, -- for when we track staging
             PRIMARY KEY (data_id, site_id)
             )""" #-- PRIMARY KEY (block_id, site_id) #-- online BOOL DEFAULT FALSE, -- for when we track staging

        # constraints added in table definition
        self.constraints.clear()
