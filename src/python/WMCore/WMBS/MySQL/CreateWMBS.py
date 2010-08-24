"""
_CreateWMBS_

Implementation of CreateWMBS for MySQL.
"""

__revision__ = "$Id: CreateWMBS.py,v 1.12 2008/09/18 22:33:28 metson Exp $"
__version__ = "$Reivison: $"

from WMCore.WMBS.CreateWMBSBase import CreateWMBSBase

class CreateWMBS(CreateWMBSBase):
    def __init__(self, logger, dbInterface):
        """
        _init_

        Call the base class's constructor and create all necessary tables,
        constraints and inserts.
        """        
        CreateWMBSBase.__init__(self, logger, dbInterface)

        self.create["01wmbs_fileset"] = \
          """CREATE TABLE wmbs_fileset (
             id          INT(11)      NOT NULL AUTO_INCREMENT,
             name        VARCHAR(255) NOT NULL,
             open        BOOLEAN      NOT NULL DEFAULT FALSE,
             last_update TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
               ON UPDATE CURRENT_TIMESTAMP,  
             PRIMARY KEY (id), UNIQUE (name))"""

        self.create["02wmbs_file_details"] = \
          """CREATE TABLE wmbs_file_details (
             id          INT(11)      NOT NULL AUTO_INCREMENT,
             lfn         VARCHAR(255) NOT NULL,
             size        INT(11),
             events      INT(11),
             first_event INT(11),
             last_event  INT(11),
             UNIQUE(lfn),
             PRIMARY KEY(id),
             INDEX (lfn))"""
             
        self.create["03wmbs_fileset_files"] = \
          """CREATE TABLE wmbs_fileset_files (
             file        INT(11)   NOT NULL,
             fileset     INT(11)   NOT NULL,
             status      ENUM ('active', 'inactive', 'invalid'),
             insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
               ON UPDATE CURRENT_TIMESTAMP,
             FOREIGN KEY(fileset) REFERENCES wmbs_fileset(id)
               ON DELETE CASCADE,
             FOREIGN KEY(file)    REFERENCES wmbs_file_details(id)
               ON DELETE CASCADE)"""

        self.create["04wmbs_file_parent"] = \
          """CREATE TABLE wmbs_file_parent (
             child  INT(11) NOT NULL,
             parent INT(11) NOT NULL,
             FOREIGN KEY (child)  REFERENCES wmbs_file_details(id)
               ON DELETE CASCADE,
             FOREIGN KEY (parent) REFERENCES wmbs_file_details(id),
             UNIQUE(child, parent))"""
        
        
        self.create["05wmbs_file_runlumi_map"] = \
          """CREATE TABLE wmbs_file_runlumi_map (
             file    INT(11) NOT NULL,
             run     INT(11) NOT NULL,
             lumi    INT(11) NOT NULL,
             FOREIGN KEY (file) REFERENCES wmbs_file_details(id)
               ON DELETE CASCADE)"""
        
        self.constraints["uniquefilerunlumi"] = \
          """CREATE UNIQUE INDEX uniq_wmbs_file_run_lumi on
             wmbs_file_runlumi_map (file, run, lumi)"""
             
        self.create["06wmbs_location"] = \
          """CREATE TABLE wmbs_location (
             id      INT(11)      NOT NULL AUTO_INCREMENT,
             se_name VARCHAR(255) NOT NULL,
             UNIQUE(se_name),
             PRIMARY KEY(id))"""
        
        self.create["07wmbs_file_location"] = \
          """CREATE TABLE wmbs_file_location (
             file     INT(11),
             location INT(11),
             UNIQUE(file, location),
             FOREIGN KEY(file)     REFERENCES wmbs_file_details(id)
               ON DELETE CASCADE,
             FOREIGN KEY(location) REFERENCES wmbs_location(id)
               ON DELETE CASCADE)"""
        
        self.create["08wmbs_workflow"] = \
          """CREATE TABLE wmbs_workflow (
            id           INT(11) NOT NULL AUTO_INCREMENT,
            spec         VARCHAR(255) NOT NULL,
            name         VARCHAR(255) NOT NULL,
            owner        VARCHAR(255) NOT NULL,
            UNIQUE(spec, name, owner),
            PRIMARY KEY (id))"""
        
        self.create["09wmbs_subscription"] = \
          """CREATE TABLE wmbs_subscription (
             id          INT(11)      NOT NULL AUTO_INCREMENT,
             fileset     INT(11)      NOT NULL,
             workflow    INT(11)      NOT NULL,
             split_algo  VARCHAR(255) NOT NULL DEFAULT 'File',
             type        ENUM('merge', 'processing'),
             last_update TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
             ON UPDATE CURRENT_TIMESTAMP,
             UNIQUE(fileset, workflow, type),   
             PRIMARY KEY(id),
             FOREIGN KEY(fileset) REFERENCES wmbs_fileset(id)
               ON DELETE CASCADE,
             FOREIGN KEY(workflow) REFERENCES wmbs_workflow(id)
               ON DELETE CASCADE)"""
          
        self.create["10wmbs_sub_files_acquired"] = \
          """CREATE TABLE wmbs_sub_files_acquired (
             subscription INT(11) NOT NULL,
             file         INT(11) NOT NULL,
             FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
               ON DELETE CASCADE,
             FOREIGN KEY (file)         REFERENCES wmbs_file_details(id))"""
        
        self.create["11wmbs_sub_files_failed"] = \
          """CREATE TABLE wmbs_sub_files_failed (
             subscription INT(11) NOT NULL,
             file         INT(11) NOT NULL,
             FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
               ON DELETE CASCADE,
             FOREIGN KEY (file)         REFERENCES wmbs_file_details(id))"""
        
        self.create["12wmbs_sub_files_complete"] = \
          """CREATE TABLE wmbs_sub_files_complete (
             subscription INT(11) NOT NULL,
             file         INT(11) NOT NULL,
             FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
               ON DELETE CASCADE,
             FOREIGN KEY (file)         REFERENCES wmbs_file_details(id))"""
        
        self.create["13wmbs_jobgroup"] = \
          """CREATE TABLE wmbs_jobgroup (
             id           INT(11)    NOT NULL AUTO_INCREMENT,
             subscription INT(11)    NOT NULL,
             last_update  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
             ON UPDATE CURRENT_TIMESTAMP,
             PRIMARY KEY (id),
             FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
               ON DELETE CASCADE)"""
        
        self.create["14wmbs_job"] = \
          """CREATE TABLE wmbs_job (
             id          INT(11)   NOT NULL AUTO_INCREMENT,
             jobgroup    INT(11)   NOT NULL,
             start       INT(11),
             completed   INT(11),
             last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
             ON UPDATE CURRENT_TIMESTAMP,
             PRIMARY KEY (id),
             FOREIGN KEY (jobgroup) REFERENCES wmbs_jobgroup(id)
               ON DELETE CASCADE)"""
        
        self.create["15wmbs_job_assoc"] = \
          """CREATE TABLE wmbs_job_assoc (
             job    INT(11) NOT NULL,
             file   INT(11) NOT NULL,
             FOREIGN KEY (job) REFERENCES wmbs_job(id)
               ON DELETE CASCADE,
             FOREIGN KEY (file) REFERENCES wmbs_file_details(id)
               ON DELETE CASCADE)"""
        
        self.constraints["uniquewfname"] = \
          "CREATE UNIQUE INDEX uniq_wf_name on wmbs_workflow (name)"
        
        self.constraints["uniquewfspecowner"] = \
          """CREATE UNIQUE INDEX uniq_wf_spec_owner on
             wmbs_workflow (spec, owner)"""
        
        self.constraints["uniquelfn"] = \
          "CREATE UNIQUE INDEX uniq_lfn on wmbs_file_details (lfn)"

    def execute(self):
        for i in self.create.keys():
            self.create[i] = self.create[i] + " ENGINE=InnoDB"
        CreateWMBSBase.execute(self)    
        