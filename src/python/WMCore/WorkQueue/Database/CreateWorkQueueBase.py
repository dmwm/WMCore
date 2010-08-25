"""
_CreateWorkQueue_

Implementation of CreateWorkQueue for MySQL.

Inherit from CreateWMBSBase, and add MySQL specific substitutions (e.g. add 
INNODB) and specific creates (e.g. for time stamp and enum fields).
"""

__revision__ = "$Id: CreateWorkQueueBase.py,v 1.23 2010/02/11 19:20:49 sryu Exp $"
__version__ = "$Revision: 1.23 $"

import threading

from WMCore.Database.DBCreator import DBCreator

from WMCore.WMException import WMException

class CreateWorkQueueBase(DBCreator):
    """
    Class to set up the WMBS schema in a MySQL database
    """
    requiredTables = ["01wq_wmspec",
                      "02wq_wmtask",
                      "03wq_data",
                      "04wq_queues",
                      "05wq_element",
                      "06wq_data_parentage",
                      "07wq_site",
                      "08wq_data_site_assoc",
                      "09wq_element_site_validation"
                      ]


    def __init__(self, logger = None, dbi = None, param = None):
        """
        _init_

        Call the DBCreator constructor and create the list of required tables.
        """
        myThread = threading.currentThread()

        if logger == None:
            logger = myThread.logger
        if dbi == None:
            dbi = myThread.dbi

        DBCreator.__init__(self, logger, dbi)


        self.create["01wq_wmspec"] = \
          """CREATE TABLE wq_wmspec (
             id          INTEGER      NOT NULL, 
             name        VARCHAR(500) NOT NULL,
             url         VARCHAR(500) NOT NULL,
             owner       VARCHAR(255) NOT NULL,
             PRIMARY KEY(id),
             UNIQUE (url)
             UNIQUE (name)
             )"""

        self.create["02wq_wmtask"] = \
          """CREATE TABLE wq_wmtask (
             id          INTEGER      NOT NULL, 
             wmspec_id  INTEGER NOT NULL,
             name       VARCHAR(500) NOT NULL,
             type       VARCHAR(255) NOT NULL,
             dbs_url    VARCHAR(500), 
             PRIMARY KEY(id),
             UNIQUE (wmspec_id, name)
             )"""
             
        self.create["03wq_data"] = \
          """CREATE TABLE wq_data (
             id             INTEGER      NOT NULL,
             name           VARCHAR(500) NOT NULL,
             PRIMARY KEY(id),
             UNIQUE (name)
             )"""

        self.create["04wq_queues"] = \
          """CREATE TABLE wq_queues (
             id               INTEGER    NOT NULL,
             url              VARCHAR(500) NOT NULL,
             PRIMARY KEY (id),
             UNIQUE(url)
             )"""
             
        self.create["05wq_element"] = \
          """CREATE TABLE wq_element (
             id               INTEGER    NOT NULL,
             wmtask_id        INTEGER    NOT NULL,
             input_id         INTEGER,
             parent_queue_id  INTEGER,
             child_queue      INTEGER,
             num_jobs         INTEGER    NOT NULL,
             priority         INTEGER    NOT NULL,
             parent_flag      INTEGER    DEFAULT 0,
             status           INTEGER    DEFAULT 0,
             subscription_id  INTEGER,
             insert_time      INTEGER    NOT NULL,
             update_time      INTEGER    NOT NULL,
             PRIMARY KEY (id)
             ) """

        self.create["06wq_data_parentage"] = \
          """CREATE TABLE wq_data_parentage (
             child        INTEGER    NOT NULL,
             parent       INTEGER    NOT NULL,
             PRIMARY KEY (child, parent)
             )"""

        self.create["07wq_site"] = \
          """CREATE TABLE wq_site (
             id          INTEGER      NOT NULL,
             name        VARCHAR(255) NOT NULL,
             PRIMARY KEY(id)
             )"""

        self.create["08wq_data_site_assoc"] = \
          """CREATE TABLE wq_data_site_assoc (
             data_id     INTEGER    NOT NULL,
             site_id      INTEGER    NOT NULL,
             PRIMARY KEY (data_id, site_id)
             )"""
        
        # whitelist and blacklist is bound to element instead of data
        # since production task also can have white and black list
        self.create["09wq_element_site_validation"] = \
          """CREATE TABLE wq_element_site_validation (
             element_id     INTEGER    NOT NULL,
             site_id      INTEGER    NOT NULL,
             valid    CHAR(1) CHECK(valid IN (0, 1)),
             PRIMARY KEY (element_id, site_id)
             )"""
                  
        self.constraints["FK_wq_data_assoc"] = \
              """ALTER TABLE wq_data_site_assoc ADD CONSTRAINT FK_wq_data_assoc
                 FOREIGN KEY(data_id) REFERENCES wq_data(id)"""

        self.constraints["FK_wq_site_assoc"] = \
              """ALTER TABLE wq_data_site_assoc ADD CONSTRAINT FK_wq_site_assoc
                 FOREIGN KEY(site_id) REFERENCES wq_site(id)"""

        self.constraints["FK_wq_data_child"] = \
              """ALTER TABLE wq_data_parentage ADD CONSTRAINT FK_wq_data_child
                 FOREIGN KEY(child) REFERENCES wq_data(id)"""

        self.constraints["FK_wq_data_parent"] = \
              """ALTER TABLE wq_data_parentage ADD CONSTRAINT FK_wq_data_parent
                 FOREIGN KEY(parent) REFERENCES wq_data(id)"""

        self.constraints["FK_wq_wmtask_element"] = \
              """ALTER TABLE wq_element ADD CONSTRAINT FK_wq_wmtask_element
                 FOREIGN KEY(wmtask_id) REFERENCES wq_wmtask(id)"""

        self.constraints["FK_wq_wmtask_wmspec"] = \
              """ALTER TABLE wq_wmtask ADD CONSTRAINT FK_wq_wq_wmtask_wmspec
                 FOREIGN KEY(wmspec_id) REFERENCES wq_wmspec(id)"""

        self.constraints["FK_wq_site_valid"] = \
              """ALTER TABLE wq_element_site_validation ADD CONSTRAINT FK_wq_site_valid
                 FOREIGN KEY(site_id) REFERENCES wq_site(id)"""
        
        self.constraints["FK_wq_element_child"] = \
              """ALTER TABLE wq_element ADD CONSTRAINT FK_wq_element_child
                 FOREIGN KEY(child_queue) REFERENCES wq_queues(id)"""
        
        self.constraints["FK_wq_element_valid"] = \
              """ALTER TABLE wq_element_site_validation ADD CONSTRAINT FK_wq_element_valid
                 FOREIGN KEY(element_id) REFERENCES wq_element(id)"""

        
#TODO : not sure whether it is better to allow input id to be null on production job
#0r have associate table with wq_element id and input id that way it will handle the multiple 
# blocks given one work queue element if necessary
#        self.constraints["FK_wq_input_element"] = \
#              """ALTER TABLE wq_element ADD CONSTRAINT FK_wq_input_element
#                 FOREIGN KEY(input_id) REFERENCES wq_data(id)"""

# temporary remove constraint
#        self.constraints["FK_wq_element_sub"] = \
#              """ALTER TABLE wq_element ADD CONSTRAINT FK_wq_element_sub
#                 FOREIGN KEY(subscription_id) REFERENCES wmbs_subscription(id)"""


    def execute(self, conn = None, transaction = None):
        """
        _execute_

        Check to make sure that all required tables have been defined.  If
        everything is in place have the DBCreator make everything.
        """
        for requiredTable in CreateWorkQueueBase.requiredTables:
            if requiredTable not in self.create.keys():
                raise WMException("The table '%s' is not defined." % \
                                  requiredTable, "WMCORE-2")

        try:
            DBCreator.execute(self, conn, transaction)
            return True
        except Exception, e:
            print "ERROR: %s" % e
            return False
