"""
_CreateWMBS_

Base class for creating the WMBS database.
"""

__revision__ = "$Id: CreateWMBSBase.py,v 1.17 2008/12/11 08:56:55 sfoulkes Exp $"
__version__ = "$Revision: 1.17 $"

import threading

from WMCore.Database.DBCreator import DBCreator

from WMCore.WMException import WMException
from WMCore.WMExceptions import WMEXCEPTION

class CreateWMBSBase(DBCreator):    
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
                
        self.requiredTables = ["01wmbs_fileset",
                               "02wmbs_file_details",
                               "03wmbs_fileset_files",
                               "04wmbs_file_parent",
                               "05wmbs_file_runlumi_map",
                               "06wmbs_location",
                               "07wmbs_file_location",
                               "08wmbs_workflow",
                               "09wmbs_subscription",
                               "09wmbs_subscription_location",
                               "10wmbs_sub_files_acquired",
                               "11wmbs_sub_files_failed",
                               "12wmbs_sub_files_complete",
                               "13wmbs_jobgroup",
                               "14wmbs_job",
                               "15wmbs_job_assoc",
                               "16wmbs_group_job_acquired",
                               "17wmbs_group_job_failed",
                               "18wmbs_group_job_complete",
                               "19wmbs_job_mask"]
        
        
        self.create["01wmbs_fileset"] = \
          """CREATE TABLE wmbs_fileset (
             id          INTEGER      PRIMARY KEY AUTO_INCREMENT,
             name        VARCHAR(255) NOT NULL,
             open        INT(1)       NOT NULL DEFAULT 0,
             last_update INTEGER      NOT NULL,
             UNIQUE (name))"""
        
        self.create["02wmbs_file_details"] = \
          """CREATE TABLE wmbs_file_details (
             id           INTEGER      PRIMARY KEY AUTO_INCREMENT,
             lfn          VARCHAR(255) NOT NULL,
             size         INTEGER,
             events       INTEGER,
             first_event  INTEGER,
             last_event   INTEGER)"""
        
        self.create["03wmbs_fileset_files"] = \
          """CREATE TABLE wmbs_fileset_files (
             file        INTEGER   NOT NULL,
             fileset     INTEGER   NOT NULL,
             insert_time INTEGER   NOT NULL,
             FOREIGN KEY(fileset) references wmbs_fileset(id))"""

        self.create["04wmbs_file_parent"] = \
          """CREATE TABLE wmbs_file_parent (
             child  INTEGER NOT NULL,
             parent INTEGER NOT NULL,
             FOREIGN KEY (child)  references wmbs_file_details(id)
               ON DELETE CASCADE,
             FOREIGN KEY (parent) references wmbs_file_details(id),
             UNIQUE(child, parent))"""  
        
        self.create["05wmbs_file_runlumi_map"] = \
          """CREATE TABLE wmbs_file_runlumi_map (
             file    INTEGER NOT NULL,
             run     INTEGER NOT NULL,
             lumi    INTEGER NOT NULL,
             FOREIGN KEY (file) references wmbs_file_details(id)
               ON DELETE CASCADE)"""
        
        self.create["06wmbs_location"] = \
          """CREATE TABLE wmbs_location (
             id      INTEGER      PRIMARY KEY AUTO_INCREMENT,
             se_name VARCHAR(255) NOT NULL,
             UNIQUE(se_name))"""
             
        self.create["07wmbs_file_location"] = \
          """CREATE TABLE wmbs_file_location (
             file     INTEGER NOT NULL,
             location INTEGER NOT NULL,
             UNIQUE(file, location),
             FOREIGN KEY(file)     REFERENCES wmbs_file_details(id)
               ON DELETE CASCADE,
             FOREIGN KEY(location) REFERENCES wmbs_location(id)
               ON DELETE CASCADE)"""
        
        self.create["08wmbs_workflow"] = \
          """CREATE TABLE wmbs_workflow (
             id           INTEGER      PRIMARY KEY AUTO_INCREMENT,
             spec         VARCHAR(255) NOT NULL,
             name         VARCHAR(255) NOT NULL,
             owner        VARCHAR(255))"""

        self.create["09wmbs_subscription"] = \
          """CREATE TABLE wmbs_subscription (
             id          INTEGER      PRIMARY KEY AUTO_INCREMENT,
             fileset     INTEGER      NOT NULL,
             workflow    INTEGER      NOT NULL,
             split_algo  VARCHAR(255) NOT NULL DEFAULT 'File',
             type        INTEGER      NOT NULL,
             last_update INTEGER      NOT NULL,
             FOREIGN KEY(fileset)  REFERENCES wmbs_fileset(id)
               ON DELETE CASCADE
             FOREIGN KEY(type)     REFERENCES wmbs_subs_type(id)
               ON DELETE CASCADE
             FOREIGN KEY(workflow) REFERENCES wmbs_workflow(id)
               ON DELETE CASCADE)""" 
               
        self.create["09wmbs_subscription_location"] = \
          """CREATE TABLE wmbs_subscription_location (
             subscription     INTEGER      NOT NULL,
             location         INTEGER      NOT NULL,
             valid            BOOLEAN      NOT NULL DEFAULT TRUE,
             FOREIGN KEY(subscription)  REFERENCES wmbs_subscription(id)
               ON DELETE CASCADE,
             FOREIGN KEY(location)     REFERENCES wmbs_location(id)
               ON DELETE CASCADE)""" 

        self.create["10wmbs_sub_files_acquired"] = \
          """CREATE TABLE wmbs_sub_files_acquired (
             subscription INTEGER NOT NULL,
             file         INTEGER NOT NULL,
             FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
               ON DELETE CASCADE,
             FOREIGN KEY (file)         REFERENCES wmbs_file_details(id))
             """

        self.create["11wmbs_sub_files_failed"] = \
          """CREATE TABLE wmbs_sub_files_failed (
             subscription INTEGER NOT NULL,
             file         INTEGER NOT NULL,
             FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
               ON DELETE CASCADE,
             FOREIGN KEY (file)         REFERENCES wmbs_file_details(id))"""

        self.create["12wmbs_sub_files_complete"] = \
          """CREATE TABLE wmbs_sub_files_complete (
          subscription INTEGER NOT NULL,
          file         INTEGER NOT NULL,
          FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
            ON DELETE CASCADE,
          FOREIGN KEY (file)         REFERENCES wmbs_file_details(id))"""

        self.create["13wmbs_jobgroup"] = \
          """CREATE TABLE wmbs_jobgroup (
             id           INTEGER      PRIMARY KEY AUTO_INCREMENT,
             subscription INTEGER      NOT NULL,
             uid          VARCHAR(255),
             output       INTEGER,
             last_update  INTEGER      NOT NULL,
             UNIQUE(uid),
             FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
               ON DELETE CASCADE,
             FOREIGN KEY (output) REFERENCES wmbs_fileset(id)
                    ON DELETE CASCADE)"""

        self.create["14wmbs_job"] = \
          """CREATE TABLE wmbs_job (
             id          INTEGER   PRIMARY KEY AUTO_INCREMENT,
             jobgroup    INTEGER   NOT NULL,
             name        VARCHAR(255),             
             last_update INTEGER   NOT NULL,
             UNIQUE(name),
             FOREIGN KEY (jobgroup) REFERENCES wmbs_jobgroup(id)
               ON DELETE CASCADE)"""

        self.create["15wmbs_job_assoc"] = \
          """CREATE TABLE wmbs_job_assoc (
             job    INTEGER NOT NULL,
             file   INTEGER NOT NULL,
             FOREIGN KEY (job)  REFERENCES wmbs_job(id)
               ON DELETE CASCADE,
             FOREIGN KEY (file) REFERENCES wmbs_file_details(id)
               ON DELETE CASCADE)"""

        self.create["16wmbs_group_job_acquired"] = \
          """CREATE TABLE wmbs_group_job_acquired (
              jobgroup INTEGER NOT NULL,
              job         INTEGER     NOT NULL,
              FOREIGN KEY (jobgroup)     REFERENCES wmbs_jobgroup(id)
                ON DELETE CASCADE,
              FOREIGN KEY (job)       REFERENCES wmbs_job(id)
                ON DELETE CASCADE)"""

        self.create["17wmbs_group_job_failed"] = \
          """CREATE TABLE wmbs_group_job_failed (
              jobgroup INTEGER NOT NULL,
              job         INTEGER     NOT NULL,
              FOREIGN KEY (jobgroup)     REFERENCES wmbs_jobgroup(id)
                ON DELETE CASCADE,
              FOREIGN KEY (job)       REFERENCES wmbs_job(id)
                ON DELETE CASCADE)"""

        self.create["18wmbs_group_job_complete"] = \
          """CREATE TABLE wmbs_group_job_complete (
              jobgroup INTEGER NOT NULL,
              job         INTEGER     NOT NULL,
              FOREIGN KEY (jobgroup)     REFERENCES wmbs_jobgroup(id)
                ON DELETE CASCADE,
              FOREIGN KEY (job)       REFERENCES wmbs_job(id)
                ON DELETE CASCADE)"""
             
        self.create["19wmbs_job_mask"] = \
          """CREATE TABLE wmbs_job_mask (
              job           INTEGER     NOT NULL,
              FirstEvent    INTEGER,
              LastEvent     INTEGER,
              FirstLumi     INTEGER,
              LastLumi      INTEGER,
              FirstRun      INTEGER,
              LastRun       INTEGER,
              inclusivemask BOOLEAN DEFAULT TRUE,
              FOREIGN KEY (job)       REFERENCES wmbs_job(id)
                ON DELETE CASCADE)"""
             
    def execute(self, conn=None, transaction=None):
        """
        _execute_
        
        Check to make sure that all required tables have been defined.  If
        everything is in place have the DBCreator make everything.
        """
        for requiredTable in self.requiredTables:
            if requiredTable not in self.create.keys():
                raise WMException("The table '%s' is not defined." % \
                                  requiredTable, "WMCORE-2")
                   
        try:
            DBCreator.execute(self, conn, transaction)
            return True
        except Exception, e:
            print "ERROR: %s" % e
            return False
