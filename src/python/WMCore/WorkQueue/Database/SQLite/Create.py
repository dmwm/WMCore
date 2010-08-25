"""
_CreateWorkQueue_

Implementation of CreateWorkQueue for SQLite.

"""

__revision__ = "$Id: Create.py,v 1.2 2009/07/17 14:25:30 swakef Exp $"
__version__ = "$Revision: 1.2 $"

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
        self.create["04wq_element"] = \
          """CREATE TABLE wq_element (
             id               INTEGER    NOT NULL,
             wmspec_id        INTEGER    NOT NULL REFERENCES wq_wmspec(id),
             block_id         INTEGER    NOT NULL REFERENCES wq_block(id),
             num_jobs         INTEGER    NOT NULL,
             priority         INTEGER    NOT NULL,
             parent_flag      INTEGER    DEFAULT 0,
             status           INTEGER    DEFAULT 0 REFERENCES wq_element_status(id),
             insert_time      INTEGER    NOT NULL,
             PRIMARY KEY (id),
             UNIQUE (wmspec_id, block_id)
             ) """

        self.create["05wq_block_parentage"] = \
          """CREATE TABLE wq_block_parentage (
             child        INTEGER    NOT NULL REFERENCES wq_block(id),
             parent       INTEGER    NOT NULL REFERENCES wq_block(id),
             PRIMARY KEY (child, parent)
             )"""

        self.create["06wq_element_subs_assoc"] = \
          """CREATE TABLE wq_element_subs_assoc (
             element_id        INTEGER    NOT NULL REFERENCES wq_element(id),
             subscription_id   INTEGER    NOT NULL REFERENCES wmbs_subscription(id),
             PRIMARY KEY (element_id, subscription_id)
             )"""
             
        # constraints added in table definition
        self.constraints.clear()