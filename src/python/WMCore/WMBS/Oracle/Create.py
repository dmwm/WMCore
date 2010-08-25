"""
_Create_

Implementation of Create for Oracle.

Inherit from CreateWMBSBase, and add Oracle specific creates to the dictionary 
at some high value.

Remove Oracle reserved words (e.g. size, file) and revise SQL used (e.g. no BOOLEAN)
"""

__revision__ = "$Id: Create.py,v 1.27 2009/12/04 20:59:35 sfoulkes Exp $"
__version__ = "$Revision: 1.27 $"

from WMCore.WMBS.CreateWMBSBase import CreateWMBSBase
from WMCore.JobStateMachine.ChangeState import Transitions

class Create(CreateWMBSBase):
    """
    Class to set up the WMBS schema in an Oracle database
    """
    sequence_tables = []
    sequence_tables.append('wmbs_fileset')
    sequence_tables.append('wmbs_file_details')
    sequence_tables.append('wmbs_location')
    sequence_tables.append('wmbs_workflow') 
    sequence_tables.append('wmbs_subscription') 
    sequence_tables.append('wmbs_jobgroup')
    sequence_tables.append('wmbs_job')
    sequence_tables.append('wmbs_job_state')
    sequence_tables.append('wmbs_checksum_type')
    
    def __init__(self, logger = None, dbi = None, params = None):
        """
        _init_

        Call the base class's constructor and create all necessary tables,
        constraints and inserts.
        """

        CreateWMBSBase.__init__(self, logger, dbi)

        tablespaceTable = ""
        tablespaceIndex = ""

        if params:
            if params.has_key("tablespace_table"):
                tablespaceTable = "TABLESPACE %s" % params["tablespace_table"]
            if params.has_key("tablespace_index"):
                tablespaceIndex = "USING INDEX TABLESPACE %s" % params["tablespace_index"]

        self.create = {}
        self.constraints = {}
        self.indexes = {}

        self.create["01wmbs_fileset"] = \
          """CREATE TABLE wmbs_fileset (
               id          INTEGER      NOT NULL,
               name        VARCHAR(255) NOT NULL,
               open        CHAR(1)      CHECK (open IN ('0', '1' )) NOT NULL,
               last_update INTEGER      NOT NULL
               ) %s""" % tablespaceTable

        self.indexes["01_pk_wmbs_fileset"] = \
          """ALTER TABLE wmbs_fileset ADD
               (CONSTRAINT wmbs_fileset_pk PRIMARY KEY (id) %s)""" % tablespaceIndex

        self.indexes["02_pk_wmbs_fileset"] = \
          """ALTER TABLE wmbs_fileset ADD
               (CONSTRAINT wmbs_fileset_unique UNIQUE (name) %s)""" % tablespaceIndex
             
        self.create["02wmbs_file_details"] = \
          """CREATE TABLE wmbs_file_details (
               id          INTEGER NOT NULL,
               lfn         VARCHAR(255) NOT NULL,
               filesize    INTEGER,
               events      INTEGER,
               first_event INTEGER,
               last_event  INTEGER,
               merged      CHAR(1) CHECK (merged IN ('0', '1' )) NOT NULL
               ) %s""" % tablespaceTable

        self.indexes["01_pk_wmbs_file_details"] = \
          """ALTER TABLE wmbs_file_details ADD
               (CONSTRAINT wmbs_file_details_pk PRIMARY KEY (id) %s)""" % tablespaceIndex

        self.indexes["02_pk_wmbs_file_details"] = \
          """ALTER TABLE wmbs_file_details ADD
               (CONSTRAINT wmbs_fildetails_unique UNIQUE (lfn) %s)""" % tablespaceIndex
             
        self.create["03wmbs_fileset_files"] = \
          """CREATE TABLE wmbs_fileset_files (
               fileid      INTEGER NOT NULL,
               fileset     INTEGER NOT NULL,
               insert_time INTEGER NOT NULL
               ) %s""" % tablespaceTable

        #self.indexes["01_pk_wmbs_fileset_files"] = \
        #  """ALTER TABLE wmbs_fileset_files ADD
        #       (CONSTRAINT wmbs_fileset_files_pk PRIMARY KEY (fileid, fileset) %s)""" % tablespaceIndex

        self.constraints["01_fk_wmbs_fileset_files"] = \
          """ALTER TABLE wmbs_fileset_files ADD
               (CONSTRAINT fk_filesetfiles_fileset FOREIGN KEY(fileset)
                  REFERENCES wmbs_fileset(id) ON DELETE CASCADE)"""

        self.constraints["01_idx_wmbs_fileset_files"] = \
          """CREATE INDEX wmbs_fileset_files_idx_fileset ON wmbs_fileset_files(fileset) %s""" % tablespaceIndex

        self.constraints["02_fk_wmbs_fileset_files"] = \
          """ALTER TABLE wmbs_fileset_files ADD
               (CONSTRAINT fk_filesetfiles_file FOREIGN KEY(fileid)
                  REFERENCES wmbs_file_details(id) ON DELETE CASCADE)"""

        self.constraints["02_idx_wmbs_fileset_files"] = \
          """CREATE INDEX wmbs_fileset_files_idx_fileid ON wmbs_fileset_files(fileid) %s""" % tablespaceIndex
             
        self.create["04wmbs_file_parent"] = \
          """CREATE TABLE wmbs_file_parent (
               child  INTEGER NOT NULL,
               parent INTEGER NOT NULL
               ) %s""" % tablespaceTable

        self.create["05wmbs_file_runlumi_map"] = \
          """CREATE TABLE wmbs_file_runlumi_map (
               fileid INTEGER NOT NULL,
               run    INTEGER NOT NULL,
               lumi   INTEGER NOT NULL
               ) %s""" % tablespaceTable

        self.constraints["01_fk_wmbs_file_runlumi_map"] = \
          """ALTER TABLE wmbs_file_runlumi_map ADD                                              
               (CONSTRAINT fk_runlumi_file FOREIGN KEY (fileid)
                  REFERENCES wmbs_file_details(id) ON DELETE CASCADE)"""

        self.constraints["01_idx_wmbs_file_runlumi_map"] = \
          """CREATE INDEX wmbs_file_runlumi_map_fileid ON wmbs_file_runlumi_map(fileid) %s""" % tablespaceIndex
        
        self.create["06wmbs_location"] = \
          """CREATE TABLE wmbs_location (
               id        INTEGER      NOT NULL,
               site_name VARCHAR(255) NOT NULL,
               job_slots INTEGER
               ) %s""" % tablespaceTable

        self.indexes["01_pk_wmbs_location"] = \
          """ALTER TABLE wmbs_location ADD
               (CONSTRAINT wmbs_location_pk PRIMARY KEY (id) %s)""" % tablespaceIndex

        self.indexes["02_pk_wmbs_location"] = \
          """ALTER TABLE wmbs_location ADD
               (CONSTRAINT wmbs_location_unique UNIQUE (site_name) %s)""" % tablespaceIndex        

        self.create["07wmbs_file_location"] = \
          """CREATE TABLE wmbs_file_location (
               fileid   INTEGER NOT NULL,
               location INTEGER NOT NULL
               ) %s""" % tablespaceTable

        self.indexes["01_pk_wmbs_file_location"] = \
          """ALTER TABLE wmbs_file_location ADD
               (CONSTRAINT wmbs_file_location_pk PRIMARY KEY (fileid, location) %s)""" % tablespaceIndex

        self.constraints["01_fk_wmbs_file_location"] = \
          """ALTER TABLE wmbs_file_location ADD                      
              (CONSTRAINT fk_location_file FOREIGN KEY(fileid)
                 REFERENCES wmbs_file_details(id) ON DELETE CASCADE)"""

        self.constraints["01_idx_wmbs_file_location"] = \
          """CREATE INDEX wmbs_file_location_fileid ON wmbs_file_location(fileid) %s""" % tablespaceIndex
        
        self.constraints["02_fk_wmbs_file_location"] = \
          """ALTER TABLE wmbs_file_location ADD                      
              (CONSTRAINT fk_location_location FOREIGN KEY(location)
                 REFERENCES wmbs_location(id) ON DELETE CASCADE)"""

        self.constraints["02_idx_wmbs_file_location"] = \
          """CREATE INDEX wmbs_file_location_location ON wmbs_file_location(location) %s""" % tablespaceIndex
         
        self.create["07wmbs_workflow"] = \
          """CREATE TABLE wmbs_workflow (
               id    INTEGER      NOT NULL,
               spec  VARCHAR(255) NOT NULL,
               name  VARCHAR(255) NOT NULL,
               task  VARCHAR(255) NOT NULL,
               owner VARCHAR(255)
               ) %s""" % tablespaceTable

        self.indexes["01_pk_wmbs_workflow"] = \
          """ALTER TABLE wmbs_workflow ADD
               (CONSTRAINT wmbs_workflow_pk PRIMARY KEY (id) %s)""" % tablespaceIndex

        self.indexes["02_pk_wmbs_workflow"] = \
          """ALTER TABLE wmbs_workflow ADD
               (CONSTRAINT wmbs_workflow_unique UNIQUE (name, task) %s)""" % tablespaceIndex

        self.create["09wmbs_workflow_output"] = \
          """CREATE TABLE wmbs_workflow_output (
               workflow_id       INTEGER      NOT NULL,
               output_identifier VARCHAR(255) NOT NULL,
               output_fileset    INTEGER      NOT NULL,
               output_parent     VARCHAR(255) 
               ) %s""" % tablespaceTable

        self.constraints["01_fk_wmbs_workflow_output"] = \
          """ALTER TABLE wmbs_workflow_output ADD
              (CONSTRAINT fk_wfoutput_workflow FOREIGN KEY(workflow_id)
                 REFERENCES wmbs_workflow(id) ON DELETE CASCADE)"""

        self.constraints["01_idx_wmbs_workflow_output"] = \
          """CREATE INDEX idx_wmbs_workf_out_workflow ON wmbs_workflow_output(workflow_id) %s""" % tablespaceIndex

        self.constraints["02_fk_wmbs_workflow_output"] = \
          """ALTER TABLE wmbs_workflow_output ADD
              (CONSTRAINT fk_wfoutput_fileset FOREIGN KEY(output_fileset)
                 REFERENCES wmbs_fileset(id) ON DELETE CASCADE)"""
        
        self.constraints["02_idx_wmbs_workflow_output"] = \
          """CREATE INDEX idx_wmbs_workf_out_fileset ON wmbs_workflow_output(output_fileset) %s""" % tablespaceIndex
        
        self.create["07wmbs_sub_types"] = \
          """CREATE TABLE wmbs_sub_types (
               id   INTEGER      NOT NULL,
               name VARCHAR(255) NOT NULL
               ) %s""" % tablespaceTable

        self.indexes["01_pk_wmbs_sub_types"] = \
          """ALTER TABLE wmbs_sub_types ADD
               (CONSTRAINT wmbs_sub_types_pk PRIMARY KEY (id) %s)""" % tablespaceIndex

        self.indexes["02_pk_wmbs_sub_types"] = \
          """ALTER TABLE wmbs_sub_types ADD
               (CONSTRAINT wmbs_sub_types_uk UNIQUE (name) %s)""" % tablespaceIndex
             
        self.create["08wmbs_subscription"] = \
          """CREATE TABLE wmbs_subscription (
               id          INTEGER      NOT NULL,
               fileset     INTEGER      NOT NULL,
               workflow    INTEGER      NOT NULL,
               split_algo  VARCHAR(255) NOT NULL,
               subtype     INTEGER      NOT NULL,
               last_update INTEGER      NOT NULL
               ) %s""" % tablespaceTable

        self.indexes["01_pk_wmbs_subscription"] = \
          """ALTER TABLE wmbs_subscription ADD
               (CONSTRAINT wmbs_subscription_pk PRIMARY KEY (id) %s)""" % tablespaceIndex

        self.constraints["01_fk_wmbs_subscription"] = \
          """ALTER TABLE wmbs_subscription ADD
               (CONSTRAINT fk_subs_fileset FOREIGN KEY(fileset)
                  REFERENCES wmbs_fileset(id) ON DELETE CASCADE)"""

        self.constraints["01_idx_wmbs_subscription"] = \
          """CREATE INDEX idx_wmbs_subscription_fileset ON wmbs_subscription(fileset) %s""" % tablespaceIndex

        self.constraints["02_fk_wmbs_subscription"] = \
          """ALTER TABLE wmbs_subscription ADD
               (CONSTRAINT fk_sub_types FOREIGN KEY(subtype)
                  REFERENCES wmbs_sub_types(id) ON DELETE CASCADE)"""

        self.constraints["02_idx_wmbs_subscription"] = \
          """CREATE INDEX idx_wmbs_subscription_subtype ON wmbs_subscription(subtype) %s""" % tablespaceIndex

        self.constraints["03_fk_wmbs_subscription"] = \
          """ALTER TABLE wmbs_subscription ADD        
               (CONSTRAINT fk_subs_workflow FOREIGN KEY(workflow)
                  REFERENCES wmbs_workflow(id) ON DELETE CASCADE)"""

        self.constraints["03_idx_wmbs_subscription"] = \
          """CREATE INDEX idx_wmbs_subscription_workflow ON wmbs_subscription(workflow) %s""" % tablespaceIndex

        self.create["09wmbs_subscription_location"] = \
          """CREATE TABLE wmbs_subscription_location (
               subscription INTEGER NOT NULL,
               location     INTEGER NOT NULL,
               valid        CHAR(1) NOT NULL
               ) %s""" % tablespaceTable

        self.constraints["01_fk_wmbs_subscription_location"] = \
          """ALTER TABLE wmbs_subscription_location ADD
               (CONSTRAINT ck_valid CHECK (valid IN ( '0', '1' )))"""

        self.constraints["02_fk_wmbs_subscription_location"] = \
          """ALTER TABLE wmbs_subscription_location ADD
               (CONSTRAINT fk_subs_loc_subscription FOREIGN KEY(subscription)
                  REFERENCES wmbs_subscription(id) ON DELETE CASCADE)"""

        self.constraints["01_idx_wmbs_subscription_location"] = \
          """CREATE INDEX idx_wmbs_subscription_loc_sub ON wmbs_subscription_location(subscription) %s""" % tablespaceIndex

        self.constraints["03_fk_wmbs_subscription_location"] = \
          """ALTER TABLE wmbs_subscription_location ADD
               (CONSTRAINT fk_subs_loc_location FOREIGN KEY(location)
                  REFERENCES wmbs_location(id) ON DELETE CASCADE)"""

        self.constraints["02_idx_wmbs_subscription_location"] = \
          """CREATE INDEX idx_wmbs_subscription_loc_loc ON wmbs_subscription_location(location) %s""" % tablespaceIndex

        self.create["10wmbs_sub_files_acquired"] = \
          """CREATE TABLE wmbs_sub_files_acquired (
               subscription INTEGER NOT NULL,
               fileid       INTEGER NOT NULL
               ) %s""" % tablespaceTable

        self.indexes["01_pk_wmbs_sub_files_acquired"] = \
          """ALTER TABLE wmbs_sub_files_acquired ADD
               (CONSTRAINT wmbs_sub_files_acquired_pk PRIMARY KEY (subscription, fileid) %s)""" % tablespaceIndex

        self.constraints["01_fk_wmbs_sub_files_acquired"] = \
          """ALTER TABLE wmbs_sub_files_acquired ADD
               (CONSTRAINT fk_subsacquired_sub FOREIGN KEY (subscription)
                  REFERENCES wmbs_subscription(id) ON DELETE CASCADE)"""

        self.constraints["02_fk_wmbs_sub_files_acquired"] = \
          """ALTER TABLE wmbs_sub_files_acquired ADD
               (CONSTRAINT fk_subsacquired_file FOREIGN KEY (fileid)
                  REFERENCES wmbs_file_details(id) ON DELETE CASCADE)"""

        self.constraints["01_idx_wmbs_sub_files_acquired"] = \
          """CREATE INDEX idx_wmbs_sub_files_acq_sub ON wmbs_sub_files_acquired(subscription) %s""" % tablespaceIndex

        self.constraints["02_idx_wmbs_sub_files_acquired"] = \
          """CREATE INDEX idx_wmbs_sub_files_acq_file ON wmbs_sub_files_acquired(fileid) %s""" % tablespaceIndex

        self.create["11wmbs_sub_files_failed"] = \
          """CREATE TABLE wmbs_sub_files_failed (
               subscription INTEGER NOT NULL,
               fileid       INTEGER NOT NULL
               ) %s""" % tablespaceTable

        self.indexes["01_pk_wmbs_sub_files_failed"] = \
          """ALTER TABLE wmbs_sub_files_failed ADD
               (CONSTRAINT wmbs_sub_files_failed_pk PRIMARY KEY (subscription, fileid) %s)""" % tablespaceIndex

        self.constraints["01_fk_wmbs_sub_files_failed"] = \
          """ALTER TABLE wmbs_sub_files_failed ADD
               (CONSTRAINT fk_subsfailed_sub FOREIGN KEY (subscription)
                  REFERENCES wmbs_subscription(id) ON DELETE CASCADE)"""

        self.constraints["02_fk_wmbs_sub_files_failed"] = \
          """ALTER TABLE wmbs_sub_files_failed ADD
               (CONSTRAINT fk_subsfailed_file FOREIGN KEY (fileid)
                  REFERENCES wmbs_file_details(id) ON DELETE CASCADE)"""

        self.constraints["01_idx_wmbs_sub_files_failed"] = \
          """CREATE INDEX idx_wmbs_sub_files_fail_sub ON wmbs_sub_files_failed(subscription) %s""" % tablespaceIndex

        self.constraints["02_idx_wmbs_sub_files_failed"] = \
          """CREATE INDEX idx_wmbs_sub_files_fail_file ON wmbs_sub_files_failed(fileid) %s""" % tablespaceIndex

        self.create["12wmbs_sub_files_complete"] = \
          """CREATE TABLE wmbs_sub_files_complete (
               subscription INTEGER NOT NULL,
               fileid       INTEGER NOT NULL
               ) %s""" % tablespaceTable

        self.indexes["01_pk_wmbs_sub_files_complete"] = \
          """ALTER TABLE wmbs_sub_files_complete ADD
               (CONSTRAINT wmbs_sub_files_complete_pk PRIMARY KEY (subscription, fileid) %s)""" % tablespaceIndex

        self.constraints["01_fk_wmbs_sub_files_complete"] = \
          """ALTER TABLE wmbs_sub_files_complete ADD
               (CONSTRAINT fk_subscomplete_sub FOREIGN KEY (subscription)
                  REFERENCES wmbs_subscription(id) ON DELETE CASCADE)"""

        self.constraints["02_fk_wmbs_sub_files_complete"] = \
          """ALTER TABLE wmbs_sub_files_complete ADD
               (CONSTRAINT fk_subscomplete_file FOREIGN KEY (fileid)
                  REFERENCES wmbs_file_details(id) ON DELETE CASCADE)"""

        self.constraints["01_idx_wmbs_sub_files_complete"] = \
          """CREATE INDEX idx_wmbs_sub_files_comp_sub ON wmbs_sub_files_complete(subscription) %s""" % tablespaceIndex

        self.constraints["02_idx_wmbs_sub_files_complete"] = \
          """CREATE INDEX idx_wmbs_sub_files_comp_file ON wmbs_sub_files_complete(fileid) %s""" % tablespaceIndex

        self.create["13wmbs_jobgroup"] = \
          """CREATE TABLE wmbs_jobgroup (
               id           INTEGER       NOT NULL,
               subscription INTEGER       NOT NULL,
               guid         VARCHAR(255),
               output       INTEGER,
               last_update  INTEGER       NOT NULL,
               location     INTEGER
               ) %s""" % tablespaceTable

        self.indexes["01_pk_wmbs_jobgroup"] = \
          """ALTER TABLE wmbs_jobgroup ADD
               (CONSTRAINT wmbs_jobgroup_pk PRIMARY KEY (id) %s)""" % tablespaceIndex

        self.indexes["02_pk_wmbs_jobgroup"] = \
          """ALTER TABLE wmbs_jobgroup ADD
               (CONSTRAINT wmbs_jobgroup_unique1 UNIQUE (output) %s)""" % tablespaceIndex

        self.indexes["03_pk_wmbs_jobgroup"] = \
          """ALTER TABLE wmbs_jobgroup ADD
               (CONSTRAINT wmbs_jobgroup_unique2 UNIQUE (guid) %s)""" % tablespaceIndex               

        self.constraints["01_fk_wmbs_jobgroup"] = \
          """ALTER TABLE wmbs_jobgroup ADD
               (CONSTRAINT fk_jobgroup_subscription FOREIGN KEY (subscription)
                  REFERENCES wmbs_subscription(id) ON DELETE CASCADE)"""

        self.constraints["02_fk_wmbs_jobgroup"] = \
          """ALTER TABLE wmbs_jobgroup ADD                  
               (CONSTRAINT fk_jobgroup_fileset FOREIGN KEY (output)
                  REFERENCES wmbs_fileset(id) ON DELETE CASCADE)"""

        self.constraints["01_idx_wmbs_sub_jobgroup"] = \
          """CREATE INDEX idx_wmbs_jobgroup_sub ON wmbs_jobgroup(subscription) %s""" % tablespaceIndex

             
        self.create["14wmbs_job_state"] = \
          """CREATE TABLE wmbs_job_state (
               id   INTEGER      NOT NULL,
               name VARCHAR(100) NOT NULL
               ) %s""" % tablespaceTable

        self.indexes["01_pk_wmbs_job_state"] = \
          """ALTER TABLE wmbs_job_state ADD
               (CONSTRAINT wmbs_job_state_pk PRIMARY KEY (id) %s)""" % tablespaceIndex

        self.create["15wmbs_job"] = \
          """CREATE TABLE wmbs_job (
               id           INTEGER       NOT NULL,
               jobgroup     INTEGER       NOT NULL,
               name         VARCHAR(255),
               state        INTEGER       NOT NULL,
               state_time   INTEGER       NOT NULL,
               retry_count  INTEGER       DEFAULT 0,
               couch_record VARCHAR(255),
               location     INTEGER,
               outcome      INTEGER       DEFAULT 0,
               cache_dir    VARCHAR(255)  DEFAULT 'None',
               fwjr_path    VARCHAR(255)
               ) %s""" % tablespaceTable

        self.indexes["01_pk_wmbs_job"] = \
          """ALTER TABLE wmbs_job ADD
               (CONSTRAINT wmbs_job_pk PRIMARY KEY (id) %s)""" % tablespaceIndex

        self.indexes["02_pk_wmbs_job"] = \
          """ALTER TABLE wmbs_job ADD
               (CONSTRAINT wmbs_job_uk UNIQUE (name) %s)""" % tablespaceIndex

        self.constraints["01_fk_wmbs_job"] = \
          """ALTER TABLE wmbs_job ADD
               (CONSTRAINT wmbs_job_fk_jobgroup FOREIGN KEY (jobgroup)
                  REFERENCES wmbs_jobgroup(id) ON DELETE CASCADE)"""

        self.constraints["01_idx_wmbs_job"] = \
          """CREATE INDEX idx_wmbs_job_jobgroup ON wmbs_job(jobgroup) %s""" % tablespaceIndex
        
        self.constraints["02_fk_wmbs_job"] = \
          """ALTER TABLE wmbs_job ADD                  
               (CONSTRAINT fk_location FOREIGN KEY (location)
                  REFERENCES wmbs_location(id))"""

        self.constraints["02_idx_wmbs_job"] = \
          """CREATE INDEX idx_wmbs_job_loc ON wmbs_job(location) %s""" % tablespaceIndex

        self.constraints["03_fk_wmbs_job"] = \
          """ALTER TABLE wmbs_job ADD
               (CONSTRAINT fk_state FOREIGN KEY (state)
                  REFERENCES wmbs_job_state(id))"""

        self.constraints["03_idx_wmbs_job"] = \
          """CREATE INDEX idx_wmbs_job_state ON wmbs_job(state) %s""" % tablespaceIndex


        self.create["16wmbs_job_assoc"] = \
          """CREATE TABLE wmbs_job_assoc (
               job    INTEGER NOT NULL,
               fileid INTEGER NOT NULL
               ) %s""" % tablespaceTable

        self.constraints["01_fk_wmbs_job_assoc"] = \
          """ALTER TABLE wmbs_job_assoc ADD
               (CONSTRAINT fk_jobassoc_job FOREIGN KEY (job)
                  REFERENCES wmbs_job(id) ON DELETE CASCADE)"""

        self.constraints["01_idx_wmbs_job_assoc"] = \
          """CREATE INDEX idx_wmbs_job_assoc_job ON wmbs_job_assoc(job) %s""" % tablespaceIndex

        self.constraints["02_fk_wmbs_job_assoc"] = \
          """ALTER TABLE wmbs_job_assoc ADD                   
               (CONSTRAINT fk_jobassoc_file FOREIGN KEY (fileid)
                  REFERENCES wmbs_file_details(id) ON DELETE CASCADE)"""

        self.constraints["02_idx_wmbs_job_assoc"] = \
          """CREATE INDEX idx_wmbs_job_assoc_file ON wmbs_job_assoc(fileid) %s""" % tablespaceIndex

        self.create["17wmbs_job_mask"] = \
          """CREATE TABLE wmbs_job_mask (
               job           INTEGER  NOT NULL,
               FirstEvent    INTEGER,
               LastEvent     INTEGER,
               FirstLumi     INTEGER,
               LastLumi      INTEGER,
               FirstRun      INTEGER,
               LastRun       INTEGER,
               inclusivemask CHAR(1) CHECK (inclusivemask IN ('Y', 'N')) NOT NULL
               ) %s""" % tablespaceTable

        self.constraints["01_fk_wmbs_job_mask"] = \
          """ALTER TABLE wmbs_job_mask ADD                   
               (CONSTRAINT fk_mask_job FOREIGN KEY (job)
                  REFERENCES wmbs_job(id) ON DELETE CASCADE)"""

        self.constraints["01_idx_wmbs_job_mask"] = \
          """CREATE INDEX idx_wmbs_job_mask_job ON wmbs_job_mask(job) %s""" % tablespaceIndex

        self.create["18wmbs_checksum_type"] = \
          """CREATE TABLE wmbs_checksum_type (
              id            INTEGER,
              type          VARCHAR(255) 
              ) %s""" % tablespaceTable

        self.indexes["01_pk_wmbs_checksum_type"] = \
          """ALTER TABLE wmbs_checksum_type ADD
               (CONSTRAINT wmbs_checksum_type_pk PRIMARY KEY (id) %s)""" % tablespaceIndex


        self.create["19wmbs_file_checksums"] = \
          """CREATE TABLE wmbs_file_checksums (
              fileid        INTEGER,
              typeid        INTEGER,
              cksum         VARCHAR(100)
              ) %s""" % tablespaceTable

        self.indexes["02_uk_wmbs_file_checksums"] = \
          """ALTER TABLE wmbs_file_checksums ADD
               (CONSTRAINT wmbs_file_checksums_uk UNIQUE (fileid, typeid) %s)""" % tablespaceIndex

        self.constraints["02_fk_wmbs_file_checksums"] = \
          """ALTER TABLE wmbs_file_checksums ADD                   
               (CONSTRAINT fk_filechecksums_cktype FOREIGN KEY (typeid)
                  REFERENCES wmbs_checksum_type(id) ON DELETE CASCADE)"""

        self.constraints["01_idx_wmbs_file_checksums"] = \
          """CREATE INDEX idx_wmbs_file_checksums_type ON wmbs_file_checksums(typeid) %s""" % tablespaceIndex

        self.constraints["03_fk_wmbs_file_checksums"] = \
          """ALTER TABLE wmbs_file_checksums ADD                   
               (CONSTRAINT fk_filechecksums_file FOREIGN KEY (fileid)
                  REFERENCES wmbs_file_details(id) ON DELETE CASCADE)"""

        self.constraints["01_idx_wmbs_file_checksums"] = \
          """CREATE INDEX idx_wmbs_file_checksums_file ON wmbs_file_checksums(fileid) %s""" % tablespaceIndex

        

        for jobState in Transitions().states():
            jobStateQuery = """INSERT INTO wmbs_job_state(id, name) VALUES
                               (wmbs_job_state_SEQ.nextval, '%s')""" % jobState
            self.inserts["job_state_%s" % jobState] = jobStateQuery

        checksumTypes = ['cksum', 'adler32', 'md5']
        for i in checksumTypes:
            checksumTypeQuery = """INSERT INTO wmbs_checksum_type (id, type) VALUES (wmbs_checksum_type_SEQ.nextval, '%s')
            """ % (i)
            self.inserts["wmbs_checksum_type_%s" % (i)] = checksumTypeQuery

          
        j = 50
        for i in self.sequence_tables:
            seqname = '%s_SEQ' % i
            self.create["%s%s" % (j, seqname)] = \
      "CREATE SEQUENCE %s start with 1 increment by 1 nomaxvalue cache 100" \
                    % seqname







    







