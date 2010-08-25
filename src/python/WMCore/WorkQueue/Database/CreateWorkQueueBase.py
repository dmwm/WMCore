"""
_CreateWorkQueue_

Implementation of CreateWorkQueue for MySQL.

Inherit from CreateWMBSBase, and add MySQL specific substitutions (e.g. add 
INNODB) and specific creates (e.g. for time stamp and enum fields).
"""

__revision__ = "$Id: CreateWorkQueueBase.py,v 1.11 2009/08/24 14:56:59 sryu Exp $"
__version__ = "$Revision: 1.11 $"

import threading

from WMCore.Database.DBCreator import DBCreator

from WMCore.WMException import WMException
from WMCore.WMExceptions import WMEXCEPTION

class CreateWorkQueueBase(DBCreator):
    """
    Class to set up the WMBS schema in a MySQL database
    """
    requiredTables = ["01wq_wmspec",
                      "02wq_block",
                      "03wq_element",
                      "04wq_block_parentage",
                      "05wq_site",
                      "06wq_block_site_assoc"
                      ]


    def __init__(self, logger = None, dbi = None):
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
             PRIMARY KEY(id),
             UNIQUE (name)
             )"""

        self.create["02wq_block"] = \
          """CREATE TABLE wq_block (
             id             INTEGER      NOT NULL,
             name           VARCHAR(500) NOT NULL,
             block_size     INTEGER      NOT NULL,
             num_files      INTEGER      NOT NULL,
             num_events      INTEGER      NOT NULL,
             PRIMARY KEY(id),
             UNIQUE (name)
             )"""
                          
        self.create["03wq_element"] = \
          """CREATE TABLE wq_element (
             id               INTEGER    NOT NULL,
             wmspec_id        INTEGER    NOT NULL,
             block_id         INTEGER    NOT NULL,
             num_jobs         INTEGER    NOT NULL,
             priority         INTEGER    NOT NULL,
             parent_flag      INTEGER    DEFAULT 0,
             status           INTEGER    DEFAULT 0,
             subscription_id  INTEGER    NOT NULL,
             insert_time      INTEGER    NOT NULL,
             PRIMARY KEY (id),
             UNIQUE (wmspec_id, block_id)
             ) """

        self.create["04wq_block_parentage"] = \
          """CREATE TABLE wq_block_parentage (
             child        INTEGER    NOT NULL,
             parent       INTEGER    NOT NULL,
             PRIMARY KEY (child, parent)
             )"""

        self.create["05wq_site"] = \
          """CREATE TABLE wq_site (
             id          INTEGER      NOT NULL,
             name        VARCHAR(255) NOT NULL,
             PRIMARY KEY(id)
             )"""
             

        self.create["06wq_block_site_assoc"] = \
          """CREATE TABLE wq_block_site_assoc (
             block_id     INTEGER    NOT NULL,
             site_id      INTEGER    NOT NULL,
             __ oracle doesn have BOOL needs some other datatype---
             -- online BOOL DEFAULT FALSE, -- for when we track staging
             PRIMARY KEY (block_id, site_id)
             )"""

        self.constraints["FK_wq_block_assoc"] = \
              """ALTER TABLE wq_block_site_assoc ADD CONSTRAINT FK_wq_block_assoc
                 FOREIGN KEY(block_id) REFERENCES wq_block(id)"""

        self.constraints["FK_wq_site_assoc"] = \
              """ALTER TABLE wq_block_site_assoc ADD CONSTRAINT FK_wq_site_assoc
                 FOREIGN KEY(site_id) REFERENCES wq_site(id)"""

        self.constraints["FK_wq_block_child"] = \
              """ALTER TABLE wq_block_parentage ADD CONSTRAINT FK_wq_block_child
                 FOREIGN KEY(child) REFERENCES wq_block(id)"""

        self.constraints["FK_wq_block_parent"] = \
              """ALTER TABLE wq_block_parentage ADD CONSTRAINT FK_wq_block_parent
                 FOREIGN KEY(parent) REFERENCES wq_block(id)"""

        self.constraints["FK_wq_wmspec_element"] = \
              """ALTER TABLE wq_element ADD CONSTRAINT FK_wq_wmspec_element
                 FOREIGN KEY(wmspec_id) REFERENCES wq_wmspec(id)"""

        self.constraints["FK_wq_block_element"] = \
              """ALTER TABLE wq_element ADD CONSTRAINT FK_wq_block_element
                 FOREIGN KEY(block_id) REFERENCES wq_block(id)"""


        #TODO: need to find the better way to handle this        
        #block magic string for no block (production work)
        #Currently production fake block name is generated
#        self.inserts["80wq_block_insert"]=\
#                """INSERT INTO wq_block (name, block_size, num_files, num_events) 
#                   VALUES ('NoBlock', 0, 0, 0)
#                """
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
