"""
_CreateWMBS_

Implementation of CreateWMBS for MySQL.

Inherit from CreateWMBSBase, and add MySQL specific substitutions (e.g. add 
INNODB) and specific creates (e.g. for time stamp and enum fields).
"""

__revision__ = "$Id: Create.py,v 1.16 2009/12/15 16:58:56 sryu Exp $"
__version__ = "$Revision: 1.16 $"

from WMCore.WMBS.CreateWMBSBase import CreateWMBSBase

class Create(CreateWMBSBase):
    """
    Class to set up the WMBS schema in a MySQL database
    """
    def __init__(self, logger = None, dbi = None, params = None):
        """
        _init_

        Call the base class's constructor and create all necessary tables,
        constraints and inserts.
        """        
        CreateWMBSBase.__init__(self, logger, dbi, params)

        self.create["01wmbs_fileset"] = \
          """CREATE TABLE wmbs_fileset (
             id          INTEGER      PRIMARY KEY AUTO_INCREMENT,
             name        VARCHAR(255) NOT NULL,
             open        INT(1)       NOT NULL DEFAULT 0,
             last_update INT(11)      NOT NULL,
             UNIQUE (name))"""
             
        self.create["03wmbs_fileset_files"] = \
          """CREATE TABLE wmbs_fileset_files (
             file        INT(11)   NOT NULL,
             fileset     INT(11)   NOT NULL,
             insert_time INT(11)   NOT NULL,
             UNIQUE(file, fileset),
             FOREIGN KEY(fileset) REFERENCES wmbs_fileset(id)
               ON DELETE CASCADE,
             FOREIGN KEY(file)    REFERENCES wmbs_file_details(id)
               ON DELETE CASCADE)"""
                     
        self.create["13wmbs_jobgroup"] = \
          """CREATE TABLE wmbs_jobgroup (
             id           INT(11)    NOT NULL AUTO_INCREMENT,
             subscription INT(11)    NOT NULL,
             uid          VARCHAR(255),    
             output       INT(11),
             last_update  INT(11)    NOT NULL,
             location     INT(11),
             PRIMARY KEY (id),
             FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
               ON DELETE CASCADE,
             FOREIGN KEY (output) REFERENCES wmbs_fileset(id)
                    ON DELETE CASCADE)"""
        
        self.constraints["uniquewfname"] = \
          "CREATE UNIQUE INDEX uniq_wf_name on wmbs_workflow (name, task)"
        
        self.constraints["uniquefilerunlumi"] = \
          """CREATE UNIQUE INDEX uniq_wmbs_file_run_lumi on
             wmbs_file_runlumi_map (file, run, lumi)"""

    def execute(self, conn = None, transaction = None):
        for i in self.create.keys():
            self.create[i] = self.create[i] + " ENGINE=InnoDB"
            self.create[i] = self.create[i].replace('INTEGER', 'INT(11)')
            
        return CreateWMBSBase.execute(self, conn, transaction)
        
