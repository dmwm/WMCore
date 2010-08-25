"""
_Create_

Implementation of Create for Oracle.

Inherit from CreateWMBSBase, and add Oracle specific creates to the dictionary 
at some high value.

Remove Oracle reserved words (e.g. size, file) and revise SQL used (e.g. no BOOLEAN)
"""

__revision__ = "$Id: CreateFNAL.py,v 1.4 2009/08/24 13:54:11 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.JobStateMachine.ChangeState import Transitions
from WMCore.WMBS.CreateWMBSBase import CreateWMBSBase

class Create(CreateWMBSBase):
    """
    Class to set up the WMBS schema in an Oracle database
    """
    sequence_tables = []
    sequence_tables.append('wmbs_fileset')
    sequence_tables.append('wmbs_file_details')
    sequence_tables.append('wmbs_location')
    sequence_tables.append('wmbs_workflow') 
    sequence_tables.append('wmbs_subs_type')
    sequence_tables.append('wmbs_subscription') 
    sequence_tables.append('wmbs_jobgroup')
    sequence_tables.append('wmbs_job')
    sequence_tables.append('wmbs_job_state')    

    def __init__(self, logger = None, dbi = None):
        """
        _init_

        Call the base class's constructor and create all necessary tables,
        constraints and inserts.
        """
        CreateWMBSBase.__init__(self, logger, dbi)
        self.requiredTables.append('09wmbs_subs_type')

        self.create["01wmbs_fileset"] = \
          """CREATE TABLE wmbs_fileset (
               id          INTEGER not null,
               name        VARCHAR(255) not null,
               open        CHAR(1) CHECK (open IN ('0', '1' )) not null,
               last_update INTEGER    not null,
               constraint  uk_filesetname unique (name))
             TABLESPACE TIER1_WMBS_DATA"""  
             
        self.create["02wmbs_file_details"] = \
          """CREATE TABLE wmbs_file_details (
               id           INTEGER not null,
               lfn          VARCHAR(255) not null,
               filesize     INTEGER,
               events       INTEGER,
               cksum        VARCHAR(100),
               first_event  INTEGER,
               last_event   INTEGER,
               merged       CHAR(1) CHECK (merged IN ('0', '1' )) NOT NULL,               
               constraint uk_filelfn unique (lfn))
             TABLESPACE TIER1_WMBS_DATA"""
             
        self.create["03wmbs_fileset_files"] = \
          """CREATE TABLE wmbs_fileset_files (
               fileid      INTEGER   not null,
               fileset     INTEGER   not null,
               insert_time INTEGER)
             TABLESPACE TIER1_WMBS_DATA"""
             
        self.create["04wmbs_file_parent"] = \
          """CREATE TABLE wmbs_file_parent (
               child  INTEGER not null,
               parent INTEGER not null)
             TABLESPACE TIER1_WMBS_DATA"""
        
        self.create["05wmbs_file_runlumi_map"] = \
          """CREATE TABLE wmbs_file_runlumi_map (
               fileid  INTEGER not null,
               run     INTEGER not null,
               lumi    INTEGER not null)
             TABLESPACE TIER1_WMBS_DATA"""
        
        self.create["06wmbs_location"] = \
          """CREATE TABLE wmbs_location (
               id          INTEGER not null,
               site_name   VARCHAR(255) not null,
               job_slots   INTEGER,
               constraint uk_sename unique (site_name))
             TABLESPACE TIER1_WMBS_DATA"""
             
        self.create["07wmbs_file_location"] = \
          """CREATE TABLE wmbs_file_location (
               fileid   INTEGER not null,
               location INTEGER not null)
             TABLESPACE TIER1_WMBS_DATA"""
         
        self.create["08wmbs_workflow"] = \
          """CREATE TABLE wmbs_workflow (
               id           INTEGER not null,
               spec         VARCHAR(255) not null,
               name         VARCHAR(255) not null,
               task         VARCHAR(255) not null,
               owner        VARCHAR(255),
               constraint uk_workflow_nameowner unique (name, owner))
             TABLESPACE TIER1_WMBS_DATA"""

        self.create["09wmbs_workflow_output"] = \
          """CREATE TABLE wmbs_workflow_output (
               workflow_id       INTEGER NOT NULL,
               output_identifier VARCHAR(255) NOT NULL,
               output_fileset    INTEGER NOT NULL)
             TABLESPACE TIER1_WMBS_DATA"""      

        self.create["09wmbs_subs_type"] = \
          """CREATE TABLE wmbs_subs_type (
               id          INTEGER not null,
               name VARCHAR(255) not null,
               constraint uk_subtype_name unique (name))
             TABLESPACE TIER1_WMBS_DATA"""
             
        self.create["09wmbs_subscription"] = \
          """CREATE TABLE wmbs_subscription (
               id          INTEGER      not null,
               fileset     INTEGER      not null,
               workflow    INTEGER      not null,
               split_algo  varchar(255) not null,
               subtype     INTEGER      not null,
               last_update INTEGER      not null)
             TABLESPACE TIER1_WMBS_DATA"""

        self.create["09wmbs_subscription_location"] = \
          """CREATE TABLE wmbs_subscription_location (
               subscription     INTEGER      NOT NULL,
               location         INTEGER      NOT NULL,
               valid            CHAR(1)      NOT NULL,
               constraint ck_valid CHECK (valid IN ( '0', '1' )))
             TABLESPACE TIER1_WMBS_DATA"""

        self.create["10wmbs_sub_files_acquired"] = \
          """CREATE TABLE wmbs_sub_files_acquired (
               subscription INTEGER not null,
               fileid       INTEGER not null)
             TABLESPACE TIER1_WMBS_DATA"""

        self.create["11wmbs_sub_files_failed"] = \
          """CREATE TABLE wmbs_sub_files_failed (
               subscription INTEGER not null,
               fileid       INTEGER not null)
             TABLESPACE TIER1_WMBS_DATA"""

        self.create["12wmbs_sub_files_complete"] = \
          """CREATE TABLE wmbs_sub_files_complete (
               subscription INTEGER not null,
               fileid       INTEGER not null)
             TABLESPACE TIER1_WMBS_DATA"""
 
        self.create["13wmbs_jobgroup"] = \
          """CREATE TABLE wmbs_jobgroup (
               id           INTEGER not null,
               subscription INTEGER not null,
               guid         VARCHAR(255),
               output       INTEGER,
               last_update  INTEGER not null,
               location     INTEGER,
               constraint uk_jobgroup_output unique (output),
               constraint uk_jobgroup_uid unique (guid))
             TABLESPACE TIER1_WMBS_DATA"""

        self.create["14wmbs_job_state"] = \
          """CREATE TABLE wmbs_job_state (
               id  INTEGER NOT NULL,
               name VARCHAR(100))
             TABLESPACE TIER1_WMBS_DATA"""  
             
        self.create["15wmbs_job"] = \
          """CREATE TABLE wmbs_job (
               id          INTEGER not null,
               jobgroup    INTEGER not null,
               name        VARCHAR(255),
               state        INTEGER not null,
               state_time   INTEGER not null,
               retry_count  INTEGER DEFAULT 0,
               couch_record VARCHAR(255),
               location     INTEGER,
               outcome      INTEGER DEFAULT 0,
               constraint uk_job_name unique (name))
             TABLESPACE TIER1_WMBS_DATA"""

        self.create["16wmbs_job_assoc"] = \
          """CREATE TABLE wmbs_job_assoc (
               job    INTEGER not null,
               fileid INTEGER not null)
             TABLESPACE TIER1_WMBS_DATA"""

        self.create["17wmbs_job_mask"] = \
      """CREATE TABLE wmbs_job_mask (
           job           INTEGER     not null,
           FirstEvent    INTEGER,
           LastEvent     INTEGER,
           FirstLumi     INTEGER,
           LastLumi      INTEGER,
           FirstRun      INTEGER,
           LastRun       INTEGER,
           inclusivemask CHAR(1) CHECK (inclusivemask IN ('Y', 'N')) not null)
         TABLESPACE TIER1_WMBS_DATA"""

        for jobState in Transitions().states():
            jobStateQuery = """INSERT INTO wmbs_job_state(id, name) VALUES
                               (wmbs_job_state_SEQ.nextval, '%s')""" % jobState
            self.inserts["job_state_%s" % jobState] = jobStateQuery

        for subType in ("Processing", "Merge", "Harvesting"):
            subTypeQuery = """INSERT INTO wmbs_subs_type (id, name) 
                          values (wmbs_subs_type_SEQ.nextval, '%s')""" % subType
            self.inserts["wmbs_subs_type_%s" % subType] = subTypeQuery

        j = 50
        for i in self.sequence_tables:
            seqname = '%s_SEQ' % i
            self.create["%s%s" % (j, seqname)] = \
      "CREATE SEQUENCE %s start with 1 increment by 1 nomaxvalue cache 100" \
                    % seqname

        # Primary keys need to be setup before foreign keys.  
        self.indexes["1_pk_wmbs_fileset"] = \
            """ALTER TABLE wmbs_fileset ADD
                 (CONSTRAINT pk_wmbs_fileset PRIMARY KEY (id)
                 USING INDEX TABLESPACE TIER1_WMBS_INDEX)"""
        
        self.indexes["1_pk_wmbs_file_details"] = \
            """ALTER TABLE wmbs_file_details ADD
                 (CONSTRAINT pk_wmbs_file_details PRIMARY KEY (id)
                  USING INDEX TABLESPACE TIER1_WMBS_INDEX)"""

        self.indexes["1_pk_wmbs_fileset_files"] = \
            """ALTER TABLE wmbs_fileset_files ADD
                 (CONSTRAINT pk_wmbs_fileset_files PRIMARY KEY (fileid, fileset)
                  USING INDEX TABLESPACE TIER1_WMBS_INDEX)"""

        self.constraints["2_fk_wmbs_fileset_files"] = \
            """ALTER TABLE wmbs_fileset_files ADD
                 (CONSTRAINT fk_filesetfiles_fileset FOREIGN KEY (fileset)
                  REFERENCES wmbs_fileset(id) ON DELETE CASCADE)"""

        self.constraints["3_fk_wmbs_fileset_files"] = \
            """ALTER TABLE wmbs_fileset_files ADD                                        
                 (CONSTRAINT fk_filesetfiles_file FOREIGN KEY (fileid)
                  REFERENCES wmbs_file_details(id) ON DELETE CASCADE)"""

        self.indexes["1_pk_wmbs_file_parent"] = \
            """ALTER TABLE wmbs_file_parent ADD
                 (CONSTRAINT pk_wmbs_file_parent PRIMARY KEY (child, parent)
                  USING INDEX TABLESPACE TIER1_WMBS_INDEX)"""

        self.constraints["2_fk_wmbs_file_parent"] = \
            """ALTER TABLE wmbs_file_parent ADD
                 (CONSTRAINT fk_fileparentage_child FOREIGN KEY (child)
                  REFERENCES wmbs_file_details(id) ON DELETE CASCADE)"""

        self.constraints["3_fk_wmbs_file_parent"] = \
            """ALTER TABLE wmbs_file_parent ADD
                 (CONSTRAINT fk_fileparentage_parent FOREIGN KEY (parent)
                  REFERENCES wmbs_file_details(id) ON DELETE CASCADE)"""

        self.indexes["1_pk_wmbs_location"] = \
            """ALTER TABLE wmbs_location ADD
                 (CONSTRAINT pk_wmbs_location PRIMARY KEY (id)
                  USING INDEX TABLESPACE TIER1_WMBS_INDEX)"""

        self.indexes["1_pk_wmbs_file_location"] = \
            """ALTER TABLE wmbs_file_location ADD
                 (CONSTRAINT pk_wmbs_file_location PRIMARY KEY (fileid, location)
                  USING INDEX TABLESPACE TIER1_WMBS_INDEX)"""

        self.constraints["2_fk_wmbs_file_location"] = \
            """ALTER TABLE wmbs_file_location ADD
                 (CONSTRAINT fk_location_file FOREIGN KEY (fileid)
                  REFERENCES wmbs_file_details(id) ON DELETE CASCADE)"""

        self.constraints["3_fk_wmbs_file_location"] = \
            """ALTER TABLE wmbs_file_location ADD
                 (CONSTRAINT fk_location_se FOREIGN KEY (location)
                  REFERENCES wmbs_location(id) ON DELETE CASCADE)"""

        self.indexes["1_pk_wmbs_workflow"] = \
            """ALTER TABLE wmbs_workflow ADD
                 (CONSTRAINT pk_wmbs_workflow PRIMARY KEY (id)
                  USING INDEX TABLESPACE TIER1_WMBS_INDEX)"""

        self.indexes["1_pk_wmbs_subs_type"] = \
            """ALTER TABLE wmbs_subs_type ADD
                 (CONSTRAINT pk_wmbs_subs_type PRIMARY KEY (id)
                  USING INDEX TABLESPACE TIER1_WMBS_INDEX)"""

        self.indexes["1_pk_wmbs_subscription"] = \
            """ALTER TABLE wmbs_subscription ADD
                 (CONSTRAINT pk_wmbs_subscription PRIMARY KEY (id)
                  USING INDEX TABLESPACE TIER1_WMBS_INDEX)"""

        self.constraints["2_fk_wmbs_subscription"] = \
            """ALTER TABLE wmbs_subscription ADD
                 (CONSTRAINT fk_subs_fileset FOREIGN KEY (fileset)
                  REFERENCES wmbs_fileset(id) ON DELETE CASCADE)"""

        self.constraints["3_fk_wmbs_subscription"] = \
            """ALTER TABLE wmbs_subscription ADD
                 (CONSTRAINT fk_subs_type FOREIGN KEY (subtype)
                  REFERENCES wmbs_subs_type(id) ON DELETE CASCADE)"""

        self.constraints["4_fk_wmbs_subscription"] = \
            """ALTER TABLE wmbs_subscription ADD
                 (CONSTRAINT fk_subs_workflow FOREIGN KEY (workflow)
                  REFERENCES wmbs_workflow(id) ON DELETE CASCADE)"""

        self.indexes["1_pk_wmbs_jobgroup"] = \
            """ALTER TABLE wmbs_jobgroup ADD
                 (CONSTRAINT pk_wmbs_jobgroup PRIMARY KEY (id)
                  USING INDEX TABLESPACE TIER1_WMBS_INDEX)""" 

        self.constraints["2_fk_wmbs_jobgroup"] = \
            """ALTER TABLE wmbs_jobgroup ADD
                 (CONSTRAINT fk_jobgroup_subscription FOREIGN KEY (subscription)
                  REFERENCES wmbs_subscription(id) ON DELETE CASCADE)"""

        self.constraints["3_fk_wmbs_jobgroup"] = \
            """ALTER TABLE wmbs_jobgroup ADD
                 (CONSTRAINT fk_jobgroup_fileset FOREIGN KEY (output)
                  REFERENCES wmbs_fileset(id) ON DELETE CASCADE)"""

        self.indexes["1_pk_wmbs_job_state"] = \
            """ALTER TABLE wmbs_job_state ADD
                 (CONSTRAINT pk_wmbs_job_state PRIMARY KEY (id)
                  USING INDEX TABLESPACE TIER1_WMBS_INDEX)"""

        self.indexes["1_pk_wmbs_job"] = \
            """ALTER TABLE wmbs_job ADD
                 (CONSTRAINT pk_wmbs_job PRIMARY KEY (id)
                  USING INDEX TABLESPACE TIER1_WMBS_INDEX)"""

        self.constraints["2_fk_wmbs_job"] = \
            """ALTER TABLE wmbs_job aDD                                            
                 (CONSTRAINT fk_job_jobgroup FOREIGN KEY (jobgroup)
                  REFERENCES wmbs_jobgroup(id) ON DELETE CASCADE)"""

        self.indexes["1_pk_wmbs_job_assoc"] = \
            """ALTER TABLE wmbs_job_assoc ADD
                 (CONSTRAINT pk_wmbs_job_assoc PRIMARY KEY (fileid, job)
                  USING INDEX TABLESPACE TIER1_WMBS_INDEX)"""

        self.constraints["2_fk_wmbs_job_assoc"] = \
            """ALTER TABLE wmbs_job_assoc ADD
                 (CONSTRAINT fk_jobassoc_job FOREIGN KEY (job)
                  REFERENCES wmbs_job(id) ON DELETE CASCADE)"""

        self.constraints["3_fk_wmbs_job_assoc"] = \
            """ALTER TABLE wmbs_job_assoc ADD
                 (CONSTRAINT fk_jobassoc_file FOREIGN KEY (fileid)
                  REFERENCES wmbs_file_details(id) ON DELETE CASCADE)"""

        self.indexes["1_pk_wmbs_file_runlumi_map"] = \
            """ALTER TABLE wmbs_file_runlumi_map ADD
                 (CONSTRAINT pk_wmbs_file_runlumi_map PRIMARY KEY (fileid, run, lumi)
                  USING INDEX TABLESPACE TIER1_WMBS_INDEX)"""

        self.constraints["2_fk_wmbs_file_runlumi_map"] = \
            """ALTER TABLE wmbs_file_runlumi_map ADD
                 (CONSTRAINT fk_runlumi_file FOREIGN KEY (fileid)
                  REFERENCES wmbs_file_details(id) ON DELETE CASCADE)"""

        self.indexes["1_pk_wmbs_job_mask"] = \
            """ALTER TABLE wmbs_job_mask ADD
                 (CONSTRAINT pk_wmbs_job_mask PRIMARY KEY (job)
                  USING INDEX TABLESPACE TIER1_WMBS_INDEX)"""

        self.constraints["2_fk_wmbs_job_mask"] = \
            """ALTER TABLE wmbs_job_mask ADD
                 (CONSTRAINT fk_mask_job FOREIGN KEY (job)
                  REFERENCES wmbs_job(id) ON DELETE CASCADE)"""

        self.constraints["2_fk_wmbs_workflow_output"] = \
            """ALTER TABLE wmbs_workflow_output ADD
                 (CONSTRAINT fk_wfoutput_workflow FOREIGN KEY (workflow_id)
                  REFERENCES wmbs_workflow(id) ON DELETE CASCADE)"""

        self.constraints["3_fk_wmbs_workflow_output"] = \
            """ALTER TABLE wmbs_workflow_output ADD
                 (CONSTRAINT fk_wfoutput_fileset FOREIGN KEY (output_fileset)
                  REFERENCES wmbs_fileset(id) ON DELETE CASCADE)"""

        self.constraints["2_fk_wmbs_subscription_location"] = \
            """ALTER TABLE wmbs_subscription_location ADD
                 (CONSTRAINT fk_subs_loc_subscription FOREIGN KEY(subscription)
                  REFERENCES wmbs_subscription(id) ON DELETE CASCADE)"""

        self.constraints["3_fk_wmbs_subscription_location"] = \
            """ALTER TABLE wmbs_subscription_location ADD
                 (CONSTRAINT fk_subs_loc_location FOREIGN KEY(location)
                  REFERENCES wmbs_location(id) ON DELETE CASCADE)"""

        self.constraints["2_fk_wmbs_sub_files_acquired"] = \
            """ALTER TABLE wmbs_sub_files_acquired ADD                                             
                 (CONSTRAINT fk_subsacquired_sub FOREIGN KEY (subscription)
                  REFERENCES wmbs_subscription(id) ON DELETE CASCADE)"""

        self.constraints["3_fk_wmbs_sub_files_acquired"] = \
            """ALTER TABLE wmbs_sub_files_acquired ADD
                 (CONSTRAINT fk_subsacquired_file FOREIGN KEY (fileid)
                  REFERENCES wmbs_file_details(id))"""

        self.constraints["2_fk_wmbs_sub_files_failed"] = \
            """ALTER TABLE wmbs_sub_files_failed ADD
                 (CONSTRAINT fk_subsfailed_sub FOREIGN KEY (subscription)
                  REFERENCES wmbs_subscription(id) ON DELETE CASCADE)"""

        self.constraints["3_fk_wmbs_sub_files_failed"] = \
            """ALTER TABLE wmbs_sub_files_failed ADD
                 (CONSTRAINT fk_subsfailed_file FOREIGN KEY (fileid)
                  REFERENCES wmbs_file_details(id))"""

        self.constraints["2_fk_wmbs_sub_files_complete"] = \
            """ALTER TABLE wmbs_sub_files_complete ADD
                (CONSTRAINT fk_subscomplete_sub FOREIGN KEY (subscription)
                 REFERENCES wmbs_subscription(id) ON DELETE CASCADE)"""

        self.constraints["3_fk_wmbs_sub_files_complete"] = \
            """ALTER TABLE wmbs_sub_files_complete ADD
                 (CONSTRAINT fk_subscomplete_file FOREIGN KEY (fileid)
                  REFERENCES wmbs_file_details(id))"""
