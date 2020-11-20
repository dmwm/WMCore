"""
_Create_

Implementation of Create for Oracle.

Inherit from CreateWMBSBase, and add Oracle specific creates to the dictionary
at some high value.

Remove Oracle reserved words (e.g. size, file) and revise SQL used (e.g. no BOOLEAN)
"""

from WMCore.JobStateMachine.Transitions import Transitions
from WMCore.WMBS.CreateWMBSBase import CreateWMBSBase


class Create(CreateWMBSBase):
    """
    Class to set up the WMBS schema in an Oracle database
    """
    sequence_tables = [
        'wmbs_fileset',
        'wmbs_file_details',
        'wmbs_location_state',
        'wmbs_location',
        'wmbs_pnns',
        'wmbs_workflow',
        'wmbs_subscription',
        'wmbs_jobgroup',
        'wmbs_job',
        'wmbs_job_state',
        'wmbs_checksum_type',
        'wmbs_sub_types',
        'wmbs_users',
        'wmbs_workunit',
    ]

    def __init__(self, logger=None, dbi=None, params=None):
        """
        _init_

        Call the base class's constructor and create all necessary tables,
        constraints and inserts.
        """
        CreateWMBSBase.__init__(self, logger, dbi)

        tablespaceTable = ""
        tablespaceIndex = ""

        if params:
            if "tablespace_table" in params:
                tablespaceTable = "TABLESPACE %s" % params["tablespace_table"]
            if "tablespace_index" in params:
                tablespaceIndex = "USING INDEX TABLESPACE %s" % params["tablespace_index"]

        self.create["01wmbs_fileset"] = \
            """CREATE TABLE wmbs_fileset (
                 id          INTEGER      NOT NULL,
                 name        VARCHAR(1250) NOT NULL,
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
                 lfn         VARCHAR(1250) NOT NULL,
                 filesize    INTEGER,
                 events      INTEGER,
                 first_event INTEGER      DEFAULT 0,
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

        # self.indexes["01_pk_wmbs_fileset_files"] = \
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

        self.constraints["01_fk_wmbs_file_parent"] = \
            """ALTER TABLE wmbs_file_parent ADD
                 (CONSTRAINT fk_fileparent_parent FOREIGN KEY(parent)
                    REFERENCES wmbs_file_details(id) ON DELETE CASCADE)"""

        self.constraints["02_fk_wmbs_file_parent"] = \
            """ALTER TABLE wmbs_file_parent ADD
                 (CONSTRAINT fk_fileparent_child FOREIGN KEY(child)
                    REFERENCES wmbs_file_details(id) ON DELETE CASCADE)"""

        self.constraints["01_idx_wmbs_file_parent"] = \
            """CREATE INDEX wmbs_file_parent_parent ON wmbs_file_parent(parent) %s""" % tablespaceIndex

        self.constraints["02_idx_wmbs_file_parent"] = \
            """CREATE INDEX wmbs_file_parent_child ON wmbs_file_parent(child) %s""" % tablespaceIndex

        self.create["05wmbs_file_runlumi_map"] = \
            """CREATE TABLE wmbs_file_runlumi_map (
                 fileid INTEGER NOT NULL,
                 run    INTEGER NOT NULL,
                 lumi   INTEGER NOT NULL,
                 num_events INTEGER
                 ) %s""" % tablespaceTable

        self.indexes["01_pk_wmbs_file_runlumi_map"] = \
            """ALTER TABLE wmbs_file_runlumi_map ADD
                 (CONSTRAINT wmbs_file_runlumi_map_pk PRIMARY KEY (fileid, run, lumi) %s)""" % tablespaceIndex

        self.constraints["01_fk_wmbs_file_runlumi_map"] = \
            """ALTER TABLE wmbs_file_runlumi_map ADD
                 (CONSTRAINT fk_runlumi_file FOREIGN KEY (fileid)
                    REFERENCES wmbs_file_details(id) ON DELETE CASCADE)"""

        self.constraints["01_idx_wmbs_file_runlumi_map"] = \
            """CREATE INDEX wmbs_file_runlumi_map_fileid ON wmbs_file_runlumi_map(fileid) %s""" % tablespaceIndex

        self.create["05wmbs_location_state"] = \
            """CREATE TABLE wmbs_location_state (
               id   INTEGER NOT NULL,
               name VARCHAR(100) NOT NULL) %s""" % tablespaceTable

        self.indexes["01_pk_wmbs_location_state"] = \
            """ALTER TABLE wmbs_location_state ADD
                 (CONSTRAINT wmbs_location_state_pk PRIMARY KEY (id) %s)""" % tablespaceIndex

        self.create["06wmbs_location"] = \
            """CREATE TABLE wmbs_location (
                 id          INTEGER      NOT NULL,
                 site_name   VARCHAR(255) NOT NULL,
                 state       INTEGER      NOT NULL,
                 cms_name    VARCHAR(255),
                 ce_name     VARCHAR(255),
                 running_slots   INTEGER,
                 pending_slots   INTEGER,
                 plugin      VARCHAR(255),
                 state_time INTEGER DEFAULT 0
                 ) %s""" % tablespaceTable

        self.indexes["01_pk_wmbs_location"] = \
            """ALTER TABLE wmbs_location ADD
                 (CONSTRAINT wmbs_location_pk PRIMARY KEY (id) %s)""" % tablespaceIndex

        self.indexes["02_pk_wmbs_location"] = \
            """ALTER TABLE wmbs_location ADD
                 (CONSTRAINT wmbs_location_unique UNIQUE (site_name) %s)""" % tablespaceIndex

        self.constraints["01_fk_wmbs_location"] = \
            """ALTER TABLE wmbs_location ADD
                 (CONSTRAINT fk_location_state FOREIGN KEY (state)
                    REFERENCES wmbs_location_state(id))"""

        self.create["06wmbs_pnns"] = \
            """CREATE TABLE wmbs_pnns (
                 id   INTEGER,
                 pnn  VARCHAR(255))"""

        self.indexes["01_pk_wmbs_pnns"] = \
            """ALTER TABLE wmbs_pnns ADD
                 (CONSTRAINT wmbs_pnns_pk PRIMARY KEY (id) %s)""" % tablespaceIndex

        self.constraints["01_uq_wmbs_pnns"] = \
            """ALTER TABLE wmbs_pnns ADD
                 (CONSTRAINT wmbs_pnns_uq UNIQUE (pnn) %s)""" % tablespaceIndex

        self.create["07wmbs_location_pnns"] = \
            """CREATE TABLE wmbs_location_pnns (
                 location  INTEGER,
                 pnn       INTEGER
                 ) %s""" % tablespaceTable

        self.constraints["01_uq_wmbs_location_pnns"] = \
            """ALTER TABLE wmbs_location_pnns ADD
                 (CONSTRAINT wmbs_location_pnns_uq UNIQUE (location, pnn) %s)""" % tablespaceIndex

        self.constraints["02_fk_wmbs_location_pnns"] = \
            """ALTER TABLE wmbs_location_pnns ADD
                 (CONSTRAINT wmbs_location_pnns_location_fk FOREIGN KEY (location)
                   REFERENCES wmbs_location(id) ON DELETE CASCADE)"""

        self.constraints["03_fk_wmbs_location_pnns"] = \
            """ALTER TABLE wmbs_location_pnns ADD
                 (CONSTRAINT wmbs_location_pnns_pnn_fk FOREIGN KEY (pnn)
                   REFERENCES wmbs_pnns(id) ON DELETE CASCADE)"""

        self.create["07wmbs_users"] = \
            """CREATE TABLE wmbs_users (
               id         INTEGER      NOT NULL,
               cert_dn    VARCHAR(255) NOT NULL,
               name_hn    VARCHAR(255),
               owner      VARCHAR(255),
               grp        VARCHAR(255),
               group_name VARCHAR(255),
               role_name  VARCHAR(255)
               ) %s""" % tablespaceTable

        self.indexes["01_pk_wmbs_users"] = \
            """ALTER TABLE wmbs_users ADD
                 (CONSTRAINT wmbs_users_pk PRIMARY KEY (id) %s)""" % tablespaceIndex

        self.indexes["02_pk_wmbs_users"] = \
            """ALTER TABLE wmbs_users ADD
                (CONSTRAINT wmbs_users_unique UNIQUE (cert_dn, group_name, role_name) %s)""" % tablespaceIndex

        self.create["07wmbs_file_location"] = \
            """CREATE TABLE wmbs_file_location (
                 fileid   INTEGER NOT NULL,
                 pnn INTEGER NOT NULL
                 ) %s""" % tablespaceTable

        self.indexes["01_pk_wmbs_file_location"] = \
            """ALTER TABLE wmbs_file_location ADD
                 (CONSTRAINT wmbs_file_location_pk PRIMARY KEY (fileid, pnn) %s)""" % tablespaceIndex

        self.constraints["01_fk_wmbs_file_location"] = \
            """ALTER TABLE wmbs_file_location ADD
                (CONSTRAINT fk_location_file FOREIGN KEY(fileid)
                   REFERENCES wmbs_file_details(id) ON DELETE CASCADE)"""

        self.constraints["01_idx_wmbs_file_location"] = \
            """CREATE INDEX wmbs_file_location_fileid ON wmbs_file_location(fileid) %s""" % tablespaceIndex

        self.constraints["02_fk_wmbs_file_location"] = \
            """ALTER TABLE wmbs_file_location ADD
                (CONSTRAINT fk_location_location FOREIGN KEY(pnn)
                   REFERENCES wmbs_pnns(id) ON DELETE CASCADE)"""

        self.constraints["02_idx_wmbs_file_location"] = \
            """CREATE INDEX wmbs_file_location_location ON wmbs_file_location(pnn) %s""" % tablespaceIndex

        self.create["07wmbs_workflow"] = \
            """CREATE TABLE wmbs_workflow (
                 id    INTEGER      NOT NULL,
                 spec  VARCHAR(700) NOT NULL,
                 name  VARCHAR(255) NOT NULL,
                 task  VARCHAR(1250) NOT NULL,
                 type  VARCHAR(255),
                 owner INTEGER      NOT NULL,
                 alt_fs_close INTEGER NOT NULL,
                 injected INTEGER   DEFAULT 0,
                 priority INTEGER   DEFAULT 0
                 ) %s""" % tablespaceTable

        self.indexes["01_pk_wmbs_workflow"] = \
            """ALTER TABLE wmbs_workflow ADD
                 (CONSTRAINT wmbs_workflow_pk PRIMARY KEY (id) %s)""" % tablespaceIndex

        self.indexes["uniquewfname"] = \
            """ALTER TABLE wmbs_workflow ADD
                 (CONSTRAINT uniq_wf_name UNIQUE (name, task) %s)""" % tablespaceIndex

        self.indexes["02_fk_wmbs_workflow"] = \
            """ALTER TABLE wmbs_workflow ADD
                (CONSTRAINT fk_workflow_users FOREIGN KEY(owner)
                   REFERENCES wmbs_users(id) ON DELETE CASCADE)"""

        self.create["08wmbs_workflow_output"] = \
            """CREATE TABLE wmbs_workflow_output (
                 workflow_id           INTEGER      NOT NULL,
                 output_identifier     VARCHAR(255) NOT NULL,
                 output_fileset        INTEGER      NOT NULL,
                 merged_output_fileset INTEGER
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

        self.constraints["03_fk_wmbs_workflow_output"] = \
            """ALTER TABLE wmbs_workflow_output ADD
                (CONSTRAINT fk_wfoutput_mfileset FOREIGN KEY(merged_output_fileset)
                   REFERENCES wmbs_fileset(id) ON DELETE CASCADE)"""

        self.constraints["02_idx_wmbs_workflow_output"] = \
            """CREATE INDEX idx_wmbs_workf_out_fileset ON wmbs_workflow_output(output_fileset) %s""" % tablespaceIndex

        self.constraints["03_idx_wmbs_workflow_output"] = \
            """CREATE INDEX idx_wmbs_workf_out_mfileset ON wmbs_workflow_output(merged_output_fileset) %s""" % tablespaceIndex

        self.create["08wmbs_sub_types"] = \
            """CREATE TABLE wmbs_sub_types (
                 id   INTEGER      NOT NULL,
                 name VARCHAR(255) NOT NULL,
                 priority INTEGER DEFAULT 0
                 ) %s""" % tablespaceTable

        self.indexes["01_pk_wmbs_sub_types"] = \
            """ALTER TABLE wmbs_sub_types ADD
                 (CONSTRAINT wmbs_sub_types_pk PRIMARY KEY (id) %s)""" % tablespaceIndex

        self.indexes["02_pk_wmbs_sub_types"] = \
            """ALTER TABLE wmbs_sub_types ADD
                 (CONSTRAINT wmbs_sub_types_uk UNIQUE (name) %s)""" % tablespaceIndex

        self.create["09wmbs_subscription"] = \
            """CREATE TABLE wmbs_subscription (
                 id          INTEGER      NOT NULL,
                 fileset     INTEGER      NOT NULL,
                 workflow    INTEGER      NOT NULL,
                 split_algo  VARCHAR(255) NOT NULL,
                 subtype     INTEGER      NOT NULL,
                 last_update INTEGER      NOT NULL,
                 finished    INTEGER      DEFAULT 0
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

        self.create["10wmbs_subscription_validation"] = \
            """CREATE TABLE wmbs_subscription_validation (
               subscription_id INTEGER NOT NULL,
               location_id     INTEGER NOT NULL,
               valid           INTEGER)"""

        self.indexes["01_pk_wmbs_sub_val"] = \
            """ALTER TABLE wmbs_subscription_validation ADD
                 (CONSTRAINT wmbs_sub_val_pk PRIMARY KEY (subscription_id, location_id) %s)""" % tablespaceIndex

        self.constraints["01_fk_wmbs_sub_val"] = \
            """ALTER TABLE wmbs_subscription_validation ADD
                (CONSTRAINT fk_sub_val FOREIGN KEY(subscription_id)
                   REFERENCES wmbs_subscription(id) ON DELETE CASCADE)"""

        self.constraints["02_fk_wmbs_sub_val"] = \
            """ALTER TABLE wmbs_subscription_validation ADD
                (CONSTRAINT fk2_sub_val FOREIGN KEY(location_id)
                   REFERENCES wmbs_location(id) ON DELETE CASCADE)"""

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

        self.create["10wmbs_sub_files_available"] = \
            """CREATE TABLE wmbs_sub_files_available (
                 subscription INTEGER NOT NULL,
                 fileid       INTEGER NOT NULL
                 ) %s""" % tablespaceTable

        self.indexes["01_pk_wmbs_sub_files_available"] = \
            """ALTER TABLE wmbs_sub_files_available ADD
                 (CONSTRAINT wmbs_sub_files_available_pk PRIMARY KEY (subscription, fileid) %s)""" % tablespaceIndex

        self.constraints["01_fk_wmbs_sub_files_available"] = \
            """ALTER TABLE wmbs_sub_files_available ADD
                 (CONSTRAINT fk_subsavailable_sub FOREIGN KEY (subscription)
                    REFERENCES wmbs_subscription(id) ON DELETE CASCADE)"""

        self.constraints["02_fk_wmbs_sub_files_available"] = \
            """ALTER TABLE wmbs_sub_files_available ADD
                 (CONSTRAINT fk_subsavailable_file FOREIGN KEY (fileid)
                    REFERENCES wmbs_file_details(id) ON DELETE CASCADE)"""

        self.constraints["01_idx_wmbs_sub_files_available"] = \
            """CREATE INDEX idx_wmbs_sub_files_ava_sub ON wmbs_sub_files_available(subscription) %s""" % tablespaceIndex

        self.constraints["02_idx_wmbs_sub_files_available"] = \
            """CREATE INDEX idx_wmbs_sub_files_ava_file ON wmbs_sub_files_available(fileid) %s""" % tablespaceIndex

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
                 cache_dir    VARCHAR(1250)  DEFAULT 'None',
                 fwjr_path    VARCHAR(1250)
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

        self.indexes["01_pk_wmbs_job_assoc"] = \
            """ALTER TABLE wmbs_job_assoc ADD
                 (CONSTRAINT wmbs_job_assoc_pk PRIMARY KEY (job, fileid) %s)""" % tablespaceIndex

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
                 inclusivemask CHAR(1) NOT NULL
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

        # Workunit table for tracking individual lumis, indices come from CreateWMBSBase.py

        self.create["21wmbs_workunit"] = (
            'CREATE TABLE wmbs_workunit('
            ' id INTEGER NOT NULL,'
            ' taskid INTEGER NOT NULL,'
            ' retry_count INTEGER DEFAULT 0,'
            ' last_unit_count INTEGER NOT NULL,'
            ' last_submit_time INTEGER NOT NULL,'
            ' status INTEGER DEFAULT 0,'
            ' PRIMARY KEY(id)'
            ') %s' % tablespaceTable
        )
        self.constraints['01_fk_wmbs_workunit'] = ('ALTER TABLE wmbs_workunit ADD '
                                                   '(CONSTRAINT wmbs_workunit_fk_taskid'
                                                   ' FOREIGN KEY(taskid) REFERENCES wmbs_workflow(id)'
                                                   ' ON DELETE CASCADE)')

        # Association table between jobs and workunits, indices come from CreateWMBSBase.py
        self.create["22wmbs_job_workunit_assoc"] = (
            'CREATE TABLE wmbs_job_workunit_assoc ('
            ' job    INTEGER NOT NULL,'
            ' workunit INTEGER NOT NULL'
            ') %s' % tablespaceTable

        )
        self.constraints['01_fk_wmbs_job_workunit_assoc'] = ('ALTER TABLE wmbs_job_workunit_assoc ADD '
                                                             '(CONSTRAINT wmbs_job_wu_assoc_fk_job'
                                                             ' FOREIGN KEY(job) REFERENCES wmbs_job(id)'
                                                             ' ON DELETE CASCADE)')
        self.constraints['02_fk_wmbs_job_workunit_assoc'] = ('ALTER TABLE wmbs_job_workunit_assoc ADD '
                                                             '(CONSTRAINT wmbs_job_wu_assoc_fk_wu'
                                                             ' FOREIGN KEY(workunit) REFERENCES wmbs_workunit(id)'
                                                             ' ON DELETE CASCADE)')

        # Association table between workunits and file/run/lumi, indices come from CreateWMBSBase.py
        self.create["23wmbs_frl_workunit_assoc"] = (
            'CREATE TABLE wmbs_frl_workunit_assoc ('
            ' workunit INTEGER NOT NULL,'
            ' firstevent INTEGER DEFAULT 0,'
            ' lastevent INTEGER DEFAULT 0,'
            ' fileid  INTEGER NOT NULL,'
            ' run     INTEGER NOT NULL,'
            ' lumi    INTEGER NOT NULL,'
            ' PRIMARY KEY(workunit, fileid, run, lumi)'
            ') %s' % tablespaceTable
        )
        self.constraints['01_fk_wmbs_frl_workunit_assoc'] = ('ALTER TABLE wmbs_frl_workunit_assoc ADD '
                                                             '(CONSTRAINT wmbs_frl_wu_assoc_fk_wu'
                                                             ' FOREIGN KEY(workunit) REFERENCES wmbs_workunit(id)'
                                                             ' ON DELETE CASCADE)')
        self.constraints['02_fk_wmbs_frl_workunit_assoc'] = ('ALTER TABLE wmbs_frl_workunit_assoc ADD '
                                                             '(CONSTRAINT wmbs_frl_wu_assoc_fk_frl'
                                                             ' FOREIGN KEY(fileid, run, lumi) '
                                                             ' REFERENCES wmbs_file_runlumi_map(fileid, run, lumi)'
                                                             ' ON DELETE CASCADE)')
        for jobState in Transitions().states():
            jobStateQuery = """INSERT INTO wmbs_job_state(id, name) VALUES
                               (wmbs_job_state_SEQ.nextval, '%s')""" % jobState
            self.inserts["job_state_%s" % jobState] = jobStateQuery

        self.subTypes = [("Processing", 0), ("Merge", 4), ("Harvesting", 5), ("Cleanup", 1),
                         ("LogCollect", 2), ("Skim", 3), ("Production", 0)]
        for pair in self.subTypes:
            subTypeQuery = """INSERT INTO wmbs_sub_types (id, name, priority)
                              VALUES (wmbs_sub_types_SEQ.nextval, '%s', %d)""" % (pair[0], pair[1])
            self.inserts["wmbs_sub_types_%s" % pair[0]] = subTypeQuery

        locationStates = ["Normal", "Down", "Draining", "Aborted"]

        for i in locationStates:
            locationStateQuery = """INSERT INTO wmbs_location_state (id, name)
                                    VALUES (wmbs_location_state_SEQ.nextval, '%s')""" % i
            self.inserts["wmbs_location_state_%s" % i] = locationStateQuery

        checksumTypes = ["cksum", "adler32", "md5"]
        for i in checksumTypes:
            checksumTypeQuery = \
                """INSERT INTO wmbs_checksum_type (id, type)
                   VALUES (wmbs_checksum_type_SEQ.nextval, '%s')""" % i
            self.inserts["wmbs_checksum_type_%s" % i] = checksumTypeQuery

        j = 50
        for i in self.sequence_tables:
            seqname = '%s_SEQ' % i
            self.create["%s%s" % (j, seqname)] = \
                """CREATE SEQUENCE %s start with 1 increment by 1 nomaxvalue cache 100""" % seqname
