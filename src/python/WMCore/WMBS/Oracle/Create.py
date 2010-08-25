"""
_Create_

Implementation of Create for Oracle.

Inherit from CreateWMBSBase, and add Oracle specific creates to the dictionary 
at some high value.

Remove Oracle reserved words (e.g. size, file) and revise SQL used (e.g. no BOOLEAN)
"""

__revision__ = "$Id: Create.py,v 1.14 2009/05/18 17:32:40 sfoulkes Exp $"
__version__ = "$Revision: 1.14 $"

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
             constraint pk_fileset primary key (id),
             constraint uk_filesetname unique (name))"""
             
        self.create["02wmbs_file_details"] = \
          """CREATE TABLE wmbs_file_details (
             id           INTEGER not null,
             lfn          VARCHAR(255) not null,
             filesize     INTEGER,
             events       INTEGER,
             cksum        VARCHAR(100),
             first_event  INTEGER,
             last_event   INTEGER,
             constraint pk_file primary key (id),
             constraint uk_filelfn unique (lfn))"""
             
        self.create["03wmbs_fileset_files"] = \
          """CREATE TABLE wmbs_fileset_files (
             fileid      INTEGER   not null,
             fileset     INTEGER   not null,
             insert_time INTEGER not null,
             constraint fk_filesetfiles_fileset
                 FOREIGN KEY(fileset) references wmbs_fileset(id)
                    ON DELETE CASCADE,
             constraint fk_filesetfiles_file
                 FOREIGN KEY(fileid) references wmbs_file_details(id)
                    ON DELETE CASCADE)"""
             
        self.create["04wmbs_file_parent"] = \
          """CREATE TABLE wmbs_file_parent (
             child  INTEGER not null,
             parent INTEGER not null,
             constraint fk_fileparentage_child
                 FOREIGN KEY (child)  references wmbs_file_details(id)
                   ON DELETE CASCADE,
             constraint fk_fileparentage_parent        
                 FOREIGN KEY (parent) references wmbs_file_details(id),
             UNIQUE(child, parent))"""  
        
        self.create["05wmbs_file_runlumi_map"] = \
          """CREATE TABLE wmbs_file_runlumi_map (
             fileid  INTEGER not null,
             run     INTEGER not null,
             lumi    INTEGER not null,
             constraint fk_runlumi_file
                 FOREIGN KEY (fileid) references wmbs_file_details(id)
                   ON DELETE CASCADE)"""
        
        self.create["06wmbs_location"] = \
          """CREATE TABLE wmbs_location (
             id          INTEGER not null,
             site_name   VARCHAR(255) not null,
             job_slots   INTEGER,
             constraint pk_sename primary key (id),
             constraint uk_sename unique (site_name))"""
             
        self.create["07wmbs_file_location"] = \
          """CREATE TABLE wmbs_file_location (
             fileid   INTEGER not null,
             location INTEGER not null,
             constraint uk_sfile_location unique (fileid, location),
             constraint fk_location_file
                 FOREIGN KEY(fileid)     REFERENCES wmbs_file_details(id)
                   ON DELETE CASCADE,
             constraint fk_location_se
                 FOREIGN KEY(location) REFERENCES wmbs_location(id)
                   ON DELETE CASCADE)"""
         
        self.create["08wmbs_workflow"] = \
          """CREATE TABLE wmbs_workflow (
             id     INTEGER not null,
             spec   VARCHAR(255) not null,
             name   VARCHAR(255) not null,
             task   VARCHAR(255) not null,
             owner  VARCHAR(255),
             
             constraint pk_workflow primary key (id),
             constraint uk_workflow_nameowner unique (name, owner))"""

        self.create["09wmbs_workflow_output"] = \
          """CREATE TABLE wmbs_workflow_output (
             workflow_id       INTEGER NOT NULL,
             output_identifier VARCHAR(255) NOT NULL,
             output_fileset    INTEGER NOT NULL,
             constraint fk_wfoutput_workflow
               FOREIGN KEY(workflow_id)  REFERENCES wmbs_workflow(id)
                 ON DELETE CASCADE,
             constraint fk_wfoutput_fileset                 
               FOREIGN KEY(output_fileset)  REFERENCES wmbs_fileset(id)
                 ON DELETE CASCADE)
             """
        
        self.create["09wmbs_subs_type"] = \
          """CREATE TABLE wmbs_subs_type (
             id          INTEGER not null,
             name VARCHAR(255) not null,
             constraint pk_subtype primary key (id),
             constraint uk_subtype_name unique (name))"""
             
        self.create["09wmbs_subscription"] = \
          """CREATE TABLE wmbs_subscription (
             id          INTEGER   not null,
             fileset     INTEGER      not null,
             workflow    INTEGER      not null,
             split_algo  varchar(255) not null,
             subtype     INTEGER      not null,
             last_update INTEGER   not null,
             constraint fk_subs_fileset
                 FOREIGN KEY(fileset)  REFERENCES wmbs_fileset(id)
                   ON DELETE CASCADE,
             constraint fk_subs_type
                 FOREIGN KEY(subtype)     REFERENCES wmbs_subs_type(id)
                   ON DELETE CASCADE,
             constraint fk_subs_workflow           
                 FOREIGN KEY(workflow) REFERENCES wmbs_workflow(id)
                   ON DELETE CASCADE,
             constraint pk_subscription primary key (id))""" 

        self.create["09wmbs_subscription_location"] = \
          """CREATE TABLE wmbs_subscription_location (
             subscription     INTEGER      NOT NULL,
             location         INTEGER      NOT NULL,
             valid            CHAR(1)    not null,
             constraint ck_valid CHECK (valid IN ( '0', '1' )),
             constraint fk_subs_loc_subscription
                 FOREIGN KEY(subscription)  REFERENCES wmbs_subscription(id)
                   ON DELETE CASCADE,
             constraint fk_subs_loc_location
                 FOREIGN KEY(location)     REFERENCES wmbs_location(id)
                   ON DELETE CASCADE)"""

        self.create["10wmbs_sub_files_acquired"] = \
          """CREATE TABLE wmbs_sub_files_acquired (
             subscription INTEGER not null,
             fileid       INTEGER not null,
             constraint fk_subsacquired_sub
                 FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
                   ON DELETE CASCADE,
             constraint fk_subsacquired_file
                 FOREIGN KEY (fileid)         REFERENCES wmbs_file_details(id))"""

        self.create["11wmbs_sub_files_failed"] = \
          """CREATE TABLE wmbs_sub_files_failed (
             subscription INTEGER not null,
             fileid       INTEGER not null,
             constraint fk_subsfailed_sub
                 FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
                   ON DELETE CASCADE,
             constraint fk_subsfailed_file
                 FOREIGN KEY (fileid)       REFERENCES wmbs_file_details(id))"""


        self.create["12wmbs_sub_files_complete"] = \
          """CREATE TABLE wmbs_sub_files_complete (
          subscription INTEGER not null,
          fileid       INTEGER not null,
          constraint fk_subscomplete_sub
             FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
               ON DELETE CASCADE,
          constraint fk_subscomplete_file
             FOREIGN KEY (fileid)         REFERENCES wmbs_file_details(id))"""

               
        self.create["13wmbs_jobgroup"] = \
          """CREATE TABLE wmbs_jobgroup (
             id           INTEGER not null,
             subscription INTEGER    not null,
             guid          VARCHAR(255),
             output       INTEGER,
             last_update  INTEGER not null,
             constraint fk_jobgroup_subscription
                 FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
                   ON DELETE CASCADE,
             constraint fk_jobgroup_fileset  
                 FOREIGN KEY (output) REFERENCES wmbs_fileset(id)
                        ON DELETE CASCADE,                        
             constraint pk_jobgroup primary key (id),
             constraint uk_jobgroup_output unique (output),
             constraint uk_jobgroup_uid unique (guid))"""
             
        self.create["14wmbs_job_state"] = \
          """CREATE TABLE wmbs_job_state (
             id  INTEGER NOT NULL,
             name VARCHAR(100),
             constraint pk_wmbs_job_state primary key (id))"""

        self.create["15wmbs_job"] = \
          """CREATE TABLE wmbs_job (
             id           INTEGER not null,
             jobgroup     INTEGER   not null,
             name         VARCHAR(255),
             state        INTEGER not null,
             state_time   INTEGER not null,
             retry_count  INTEGER DEFAULT 0,
             couch_record VARCHAR(255),
             location     INTEGER,
             outcome      INTEGER DEFAULT 0,
             constraint fk_job_jobgroup
                 FOREIGN KEY (jobgroup) REFERENCES wmbs_jobgroup(id)
                   ON DELETE CASCADE,
             constraint fk_location
                 FOREIGN KEY (location) REFERENCES wmbs_location(id),
             constraint fk_state
                 FOREIGN KEY (state) REFERENCES wmbs_job_state(id),
             constraint pk_job PRIMARY KEY(id),
             constraint uk_job_name UNIQUE(name))"""     

        self.create["16wmbs_job_assoc"] = \
          """CREATE TABLE wmbs_job_assoc (
             job    INTEGER not null,
             fileid  INTEGER not null,
             constraint fk_jobassoc_job
                 FOREIGN KEY (job)  REFERENCES wmbs_job(id)
                   ON DELETE CASCADE,
             constraint fk_jobassoc_file
                 FOREIGN KEY (fileid) REFERENCES wmbs_file_details(id)
                   ON DELETE CASCADE)"""

        self.create["17wmbs_job_mask"] = \
      """CREATE TABLE wmbs_job_mask (
          job           INTEGER     not null,
          FirstEvent    INTEGER,
          LastEvent     INTEGER,
          FirstLumi     INTEGER,
          LastLumi      INTEGER,
          FirstRun      INTEGER,
          LastRun       INTEGER,
          inclusivemask CHAR(1) CHECK (inclusivemask IN ('Y', 'N')) not null,
          constraint fk_mask_job
              FOREIGN KEY (job) REFERENCES wmbs_job(id)
                ON DELETE CASCADE)"""

        for jobState in Transitions().states():
            jobStateQuery = """INSERT INTO wmbs_job_state(id, name) VALUES
                               (wmbs_job_state_SEQ.nextval, '%s')""" % jobState
            self.inserts["job_state_%s" % jobState] = jobStateQuery
          
        for subType in ("Processing", "Merge"):
            subTypeQuery = """INSERT INTO wmbs_subs_type (id, name) 
                          values (wmbs_subs_type_SEQ.nextval, '%s')""" % subType
            self.inserts["wmbs_subs_type_%s" % subType] = subTypeQuery

        j = 50
        for i in self.sequence_tables:
            seqname = '%s_SEQ' % i
            self.create["%s%s" % (j, seqname)] = \
      "CREATE SEQUENCE %s start with 1 increment by 1 nomaxvalue cache 100" \
                    % seqname
