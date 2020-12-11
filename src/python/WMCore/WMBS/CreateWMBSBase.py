"""
_CreateWMBS_

Base class for creating the WMBS database.
"""

import threading

from WMCore.Database.DBCreator import DBCreator
from WMCore.JobStateMachine.Transitions import Transitions
from WMCore.WMException import WMException


class CreateWMBSBase(DBCreator):
    def __init__(self, logger=None, dbi=None, params=None):
        """
        _init_

        Call the DBCreator constructor and create the list of required tables.
        """
        myThread = threading.currentThread()

        if logger is None:
            logger = myThread.logger
        if dbi is None:
            dbi = myThread.dbi

        tablespaceIndex = ""
        if params:
            if "tablespace_index" in params:
                tablespaceIndex = "USING INDEX TABLESPACE %s" % params["tablespace_index"]

        DBCreator.__init__(self, logger, dbi)

        self.requiredTables = ["01wmbs_fileset",
                               "02wmbs_file_details",
                               "03wmbs_fileset_files",
                               "04wmbs_file_parent",
                               "05wmbs_file_runlumi_map",
                               "05wmbs_location_state",
                               "06wmbs_location",
                               "06wmbs_pnns",
                               "07wmbs_location_pnns",
                               "07wmbs_file_location",
                               "07wmbs_users",
                               "07wmbs_workflow",
                               "08wmbs_sub_types",
                               "08wmbs_workflow_output",
                               "09wmbs_subscription",
                               "10wmbs_subscription_validation",
                               "10wmbs_sub_files_acquired",
                               "10wmbs_sub_files_available",
                               "11wmbs_sub_files_failed",
                               "12wmbs_sub_files_complete",
                               "13wmbs_jobgroup",
                               "14wmbs_job_state",
                               "15wmbs_job",
                               "16wmbs_job_assoc",
                               "17wmbs_job_mask",
                               "18wmbs_checksum_type",
                               "19wmbs_file_checksums",
                               "21wmbs_workunit",
                               "22wmbs_job_workunit_assoc",
                               "23wmbs_frl_workunit_assoc",
                               ]

        self.create["01wmbs_fileset"] = \
            """CREATE TABLE wmbs_fileset (
               id          INTEGER      PRIMARY KEY AUTO_INCREMENT,
               name        VARCHAR(1250) NOT NULL,
               open        INT(1)       NOT NULL DEFAULT 0,
               last_update INTEGER      NOT NULL,
               UNIQUE (name))"""

        self.create["02wmbs_file_details"] = \
            """CREATE TABLE wmbs_file_details (
               id           INTEGER      PRIMARY KEY AUTO_INCREMENT,
               lfn          VARCHAR(1250) NOT NULL,
               filesize     BIGINT,
               events       BIGINT UNSIGNED,
               first_event  BIGINT       UNSIGNED NOT NULL DEFAULT 0,
               merged       INT(1)       NOT NULL DEFAULT 0,
               UNIQUE (lfn))"""

        self.create["03wmbs_fileset_files"] = \
            """CREATE TABLE wmbs_fileset_files (
               fileid      INTEGER   NOT NULL,
               fileset     INTEGER   NOT NULL,
               insert_time INTEGER   NOT NULL,
               UNIQUE (fileid, fileset),
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
               fileid  INTEGER NOT NULL,
               run     INTEGER NOT NULL,
               lumi    INTEGER NOT NULL,
               num_events  BIGINT UNSIGNED,
               PRIMARY KEY (fileid, run, lumi),
               FOREIGN KEY (fileid) references wmbs_file_details(id)
                 ON DELETE CASCADE)"""

        self.create["05wmbs_location_state"] = \
            """CREATE TABLE wmbs_location_state (
               id   INTEGER PRIMARY KEY AUTO_INCREMENT,
               name VARCHAR(100) NOT NULL)"""

        self.create["06wmbs_location"] = \
            """CREATE TABLE wmbs_location (
               id          INTEGER      PRIMARY KEY AUTO_INCREMENT,
               site_name   VARCHAR(255) NOT NULL,
               state       INTEGER NOT NULL,
               cms_name    VARCHAR(255),
               ce_name     VARCHAR(255),
               running_slots   INTEGER,
               pending_slots   INTEGER,
               plugin      VARCHAR(255),
               state_time  INTEGER DEFAULT 0,
               UNIQUE(site_name),
               FOREIGN KEY (state) REFERENCES wmbs_location_state(id))"""

        self.create["06wmbs_pnns"] = \
            """CREATE TABLE wmbs_pnns (
                 id   INTEGER      PRIMARY KEY AUTO_INCREMENT,
                 pnn    VARCHAR(255),
                 UNIQUE(pnn))"""

        self.create["07wmbs_location_pnns"] = \
            """CREATE TABLE wmbs_location_pnns (
                 location   INTEGER,
                 pnn    INTEGER,
                 UNIQUE(location, pnn),
                 FOREIGN KEY (location) REFERENCES wmbs_location(id)
                   ON DELETE CASCADE,
                 FOREIGN KEY (pnn) REFERENCES wmbs_pnns(id)
                   ON DELETE CASCADE)"""

        self.create["07wmbs_users"] = \
            """CREATE TABLE wmbs_users (
               id        INTEGER      PRIMARY KEY AUTO_INCREMENT,
               cert_dn   VARCHAR(255) NOT NULL,
               name_hn   VARCHAR(255),
               owner     VARCHAR(255),
               grp       VARCHAR(255),
               group_name     VARCHAR(255),
               role_name      VARCHAR(255),
               UNIQUE(cert_dn, group_name, role_name))"""

        self.create["07wmbs_file_location"] = \
            """CREATE TABLE wmbs_file_location (
               fileid   INTEGER NOT NULL,
               pnn      INTEGER NOT NULL,
               UNIQUE(fileid, pnn),
               FOREIGN KEY(fileid)   REFERENCES wmbs_file_details(id)
                 ON DELETE CASCADE,
               FOREIGN KEY(pnn) REFERENCES wmbs_pnns(id)
                 ON DELETE CASCADE)"""

        self.create["07wmbs_workflow"] = \
            """CREATE TABLE wmbs_workflow (
               id           INTEGER          PRIMARY KEY AUTO_INCREMENT,
               spec         VARCHAR(700)     NOT NULL,
               name         VARCHAR(255)     NOT NULL,
               task         VARCHAR(1250)     NOT NULL,
               type         VARCHAR(255),
               owner        INTEGER          NOT NULL,
               alt_fs_close INT(1)           NOT NULL,
               injected     INT(1)           DEFAULT 0,
               priority     INTEGER UNSIGNED DEFAULT 0,
               FOREIGN KEY (owner)
               REFERENCES wmbs_users(id) ON DELETE CASCADE) """

        self.indexes["03_pk_wmbs_workflow"] = \
            """ALTER TABLE wmbs_workflow ADD
                 (CONSTRAINT wmbs_workflow_unique UNIQUE (name, spec, task))"""

        self.create["08wmbs_workflow_output"] = \
            """CREATE TABLE wmbs_workflow_output (
               workflow_id           INTEGER NOT NULL,
               output_identifier     VARCHAR(255) NOT NULL,
               output_fileset        INTEGER NOT NULL,
               merged_output_fileset INTEGER,
               FOREIGN KEY(workflow_id)  REFERENCES wmbs_workflow(id)
                 ON DELETE CASCADE,
               FOREIGN KEY(output_fileset)  REFERENCES wmbs_fileset(id)
                 ON DELETE CASCADE,
               FOREIGN KEY(merged_output_fileset)  REFERENCES wmbs_fileset(id)
                 ON DELETE CASCADE)
               """

        self.create["08wmbs_sub_types"] = \
            """CREATE TABLE wmbs_sub_types (
                 id   INTEGER      PRIMARY KEY AUTO_INCREMENT,
                 name VARCHAR(255) NOT NULL,
                 priority INTEGER DEFAULT 0,
                 UNIQUE(name))"""

        self.create["09wmbs_subscription"] = \
            """CREATE TABLE wmbs_subscription (
               id          INTEGER      PRIMARY KEY AUTO_INCREMENT,
               fileset     INTEGER      NOT NULL,
               workflow    INTEGER      NOT NULL,
               split_algo  VARCHAR(255) NOT NULL,
               subtype     INTEGER      NOT NULL,
               last_update INTEGER      NOT NULL,
               finished    INT(1)       DEFAULT 0,
               FOREIGN KEY(fileset)
               REFERENCES wmbs_fileset(id) ON DELETE CASCADE,
               FOREIGN KEY(workflow)
               REFERENCES wmbs_workflow(id) ON DELETE CASCADE,
               FOREIGN KEY(subtype)
               REFERENCES wmbs_sub_types(id) ON DELETE CASCADE)"""

        self.create["10wmbs_subscription_validation"] = \
            """CREATE TABLE wmbs_subscription_validation (
               subscription_id INTEGER NOT NULL,
               location_id     INTEGER NOT NULL,
               valid           INTEGER,
               UNIQUE (subscription_id, location_id),
               FOREIGN KEY(subscription_id) REFERENCES wmbs_subscription(id)
                 ON DELETE CASCADE,
               FOREIGN KEY(location_id) REFERENCES wmbs_location(id)
                 ON DELETE CASCADE)"""

        self.create["10wmbs_sub_files_acquired"] = \
            """CREATE TABLE wmbs_sub_files_acquired (
               subscription INTEGER NOT NULL,
               fileid       INTEGER NOT NULL,
               PRIMARY KEY (subscription, fileid),
               FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
                 ON DELETE CASCADE,
               FOREIGN KEY (fileid)       REFERENCES wmbs_file_details(id)
                 ON DELETE CASCADE)
               """

        self.create["10wmbs_sub_files_available"] = \
            """CREATE TABLE wmbs_sub_files_available (
               subscription INTEGER NOT NULL,
               fileid       INTEGER NOT NULL,
               PRIMARY KEY (subscription, fileid),
               FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
                 ON DELETE CASCADE,
               FOREIGN KEY (fileid)       REFERENCES wmbs_file_details(id)
                 ON DELETE CASCADE)
               """

        self.create["11wmbs_sub_files_failed"] = \
            """CREATE TABLE wmbs_sub_files_failed (
               subscription INTEGER NOT NULL,
               fileid       INTEGER NOT NULL,
               PRIMARY KEY (subscription, fileid),
               FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
                 ON DELETE CASCADE,
               FOREIGN KEY (fileid)       REFERENCES wmbs_file_details(id)
                 ON DELETE CASCADE)"""

        self.create["12wmbs_sub_files_complete"] = \
            """CREATE TABLE wmbs_sub_files_complete (
               subscription INTEGER NOT NULL,
               fileid       INTEGER NOT NULL,
               PRIMARY KEY (subscription, fileid),
               FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
                 ON DELETE CASCADE,
               FOREIGN KEY (fileid)       REFERENCES wmbs_file_details(id)
                 ON DELETE CASCADE)"""

        self.create["13wmbs_jobgroup"] = \
            """CREATE TABLE wmbs_jobgroup (
               id           INTEGER      PRIMARY KEY AUTO_INCREMENT,
               subscription INTEGER      NOT NULL,
               guid         VARCHAR(255),
               output       INTEGER,
               last_update  INTEGER      NOT NULL,
               location     INTEGER,
               UNIQUE(guid),
               FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
                 ON DELETE CASCADE,
               FOREIGN KEY (output) REFERENCES wmbs_fileset(id)
                      ON DELETE CASCADE)"""

        self.create["14wmbs_job_state"] = \
            """CREATE TABLE wmbs_job_state (
               id        INTEGER       PRIMARY KEY AUTO_INCREMENT,
               name      VARCHAR(100))"""

        self.create["15wmbs_job"] = \
            """CREATE TABLE wmbs_job (
               id           INTEGER       PRIMARY KEY AUTO_INCREMENT,
               jobgroup     INTEGER       NOT NULL,
               name         VARCHAR(255),
               state        INTEGER       NOT NULL,
               state_time   INTEGER       NOT NULL,
               retry_count  INTEGER       DEFAULT 0,
               couch_record VARCHAR(255),
               location     INTEGER,
               outcome      INTEGER       DEFAULT 0,
               cache_dir    VARCHAR(1250)  DEFAULT 'None',
               fwjr_path    VARCHAR(1250),
               FOREIGN KEY (jobgroup)
               REFERENCES wmbs_jobgroup(id) ON DELETE CASCADE,
               FOREIGN KEY (state) REFERENCES wmbs_job_state(id),
               FOREIGN KEY (location) REFERENCES wmbs_location(id))"""

        self.indexes["03_pk_wmbs_job"] = \
            """ALTER TABLE wmbs_job ADD
                 (CONSTRAINT wmbs_job_unique UNIQUE (name, cache_dir, fwjr_path))"""

        self.create["16wmbs_job_assoc"] = \
            """CREATE TABLE wmbs_job_assoc (
               job    INTEGER NOT NULL,
               fileid INTEGER NOT NULL,
               FOREIGN KEY (job)  REFERENCES wmbs_job(id)
                 ON DELETE CASCADE,
               FOREIGN KEY (fileid) REFERENCES wmbs_file_details(id)
                 ON DELETE CASCADE)"""

        self.create["17wmbs_job_mask"] = \
            """CREATE TABLE wmbs_job_mask (
                job           INTEGER     NOT NULL,
                FirstEvent    BIGINT,
                LastEvent     BIGINT,
                FirstLumi     INTEGER,
                LastLumi      INTEGER,
                FirstRun      INTEGER,
                LastRun       INTEGER,
                inclusivemask BOOLEAN DEFAULT TRUE,
                FOREIGN KEY (job)       REFERENCES wmbs_job(id)
                  ON DELETE CASCADE)"""

        self.create["18wmbs_checksum_type"] = \
            """CREATE TABLE wmbs_checksum_type (
                id            INTEGER      PRIMARY KEY AUTO_INCREMENT,
                type          VARCHAR(255) )
                """

        self.create["19wmbs_file_checksums"] = \
            """CREATE TABLE wmbs_file_checksums (
                fileid        INTEGER,
                typeid        INTEGER,
                cksum         VARCHAR(100),
                UNIQUE (fileid, typeid),
                FOREIGN KEY (typeid) REFERENCES wmbs_checksum_type(id)
                  ON DELETE CASCADE,
                FOREIGN KEY (fileid) REFERENCES wmbs_file_details(id)
                  ON DELETE CASCADE)"""

        # Workunit table for tracking individual lumis
        self.create["21wmbs_workunit"] = (
            'CREATE TABLE wmbs_workunit('
            ' id INTEGER PRIMARY KEY AUTO_INCREMENT,'
            ' taskid INTEGER NOT NULL,'
            ' retry_count INTEGER DEFAULT 0,'
            ' last_unit_count INTEGER NOT NULL,'
            ' last_submit_time INTEGER NOT NULL,'
            ' status INT(1) DEFAULT 0,'
            ' FOREIGN KEY(taskid) REFERENCES wmbs_workflow(id) ON DELETE CASCADE)'
        )
        self.constraints["01_idx_wmbs_workunit"] = ('CREATE INDEX idx_wmbs_workunit_task'
                                                    ' ON wmbs_workunit(taskid) %s' % tablespaceIndex)
        self.constraints["02_idx_wmbs_workunit"] = ('CREATE INDEX idx_wmbs_workunit_status'
                                                    ' ON wmbs_workunit(status) %s' % tablespaceIndex)

        # Association table between jobs and workunits
        self.create["22wmbs_job_workunit_assoc"] = (
            'CREATE TABLE wmbs_job_workunit_assoc ('
            ' job    INTEGER NOT NULL,'
            ' workunit INTEGER NOT NULL,'
            ' FOREIGN KEY (job)  REFERENCES wmbs_job(id) ON DELETE CASCADE,'
            ' FOREIGN KEY (workunit) REFERENCES wmbs_workunit(id) ON DELETE CASCADE)'
        )
        self.constraints["01_idx_wmbs_job_workunit_assoc"] = ('CREATE INDEX idx_wmbs_job_wu_assoc_job'
                                                              ' ON wmbs_job_workunit_assoc(job) %s' % tablespaceIndex)
        self.constraints["02_idx_wmbs_job_workunit_assoc"] = ('CREATE INDEX idx_wmbs_job_wu_assoc_wu'
                                                              ' ON wmbs_job_workunit_assoc(workunit) %s' % tablespaceIndex)

        # Association table between work units and file/run/lumi triplets
        self.create["23wmbs_frl_workunit_assoc"] = (
            'CREATE TABLE wmbs_frl_workunit_assoc ('
            ' workunit INTEGER NOT NULL,'
            ' firstevent INTEGER DEFAULT 0,'
            ' lastevent INTEGER DEFAULT 0,'
            ' fileid  INTEGER NOT NULL,'
            ' run     INTEGER NOT NULL,'
            ' lumi    INTEGER NOT NULL,'
            ' PRIMARY KEY (workunit, fileid, run, lumi),'
            ' FOREIGN KEY (workunit) REFERENCES wmbs_workunit(id) ON DELETE CASCADE,'
            ' FOREIGN KEY (fileid, run, lumi) REFERENCES wmbs_file_runlumi_map(fileid, run, lumi) ON DELETE CASCADE '
            ')'
        )
        self.constraints["01_idx_wmbs_frl_workunit_assoc"] = ('CREATE INDEX idx_wmbs_frl_wu_assoc_wu'
                                                              ' ON wmbs_frl_workunit_assoc(workunit) %s' % tablespaceIndex)
        self.constraints["02_idx_wmbs_frl_workunit_assoc"] = ('CREATE INDEX idx_wmbs_frl_wu_assoc_frl'
                                                              ' ON wmbs_frl_workunit_assoc(fileid, run, lumi) %s' % tablespaceIndex)

        # Back to other indices and constraints for tables 1-20
        self.constraints["01_idx_wmbs_fileset_files"] = \
            """CREATE INDEX wmbs_fileset_files_idx_fileset ON wmbs_fileset_files(fileset) %s""" % tablespaceIndex

        self.constraints["02_idx_wmbs_fileset_files"] = \
            """CREATE INDEX wmbs_fileset_files_idx_fileid ON wmbs_fileset_files(fileid) %s""" % tablespaceIndex

        self.constraints["01_idx_wmbs_file_runlumi_map"] = \
            """CREATE INDEX wmbs_file_runlumi_map_fileid ON wmbs_file_runlumi_map(fileid) %s""" % tablespaceIndex

        self.constraints["01_idx_wmbs_file_location"] = \
            """CREATE INDEX wmbs_file_location_fileid ON wmbs_file_location(fileid) %s""" % tablespaceIndex

        self.constraints["02_idx_wmbs_file_location"] = \
            """CREATE INDEX wmbs_file_location_pnn ON wmbs_file_location(pnn) %s""" % tablespaceIndex

        self.constraints["01_idx_wmbs_file_parent"] = \
            """CREATE INDEX wmbs_file_parent_parent ON wmbs_file_parent(parent) %s""" % tablespaceIndex

        self.constraints["02_idx_wmbs_file_parent"] = \
            """CREATE INDEX wmbs_file_parent_child ON wmbs_file_parent(child) %s""" % tablespaceIndex

        self.constraints["01_idx_wmbs_workflow_output"] = \
            """CREATE INDEX idx_wmbs_workf_out_workflow ON wmbs_workflow_output(workflow_id) %s""" % tablespaceIndex

        self.constraints["02_idx_wmbs_workflow_output"] = \
            """CREATE INDEX idx_wmbs_workf_out_fileset ON wmbs_workflow_output(output_fileset) %s""" % tablespaceIndex

        self.constraints["03_idx_wmbs_workflow_output"] = \
            """CREATE INDEX idx_wmbs_workf_mout_fileset ON wmbs_workflow_output(merged_output_fileset) %s""" % tablespaceIndex

        self.constraints["01_idx_wmbs_subscription"] = \
            """CREATE INDEX idx_wmbs_subscription_fileset ON wmbs_subscription(fileset) %s""" % tablespaceIndex

        self.constraints["02_idx_wmbs_subscription"] = \
            """CREATE INDEX idx_wmbs_subscription_subtype ON wmbs_subscription(subtype) %s""" % tablespaceIndex

        self.constraints["03_idx_wmbs_subscription"] = \
            """CREATE INDEX idx_wmbs_subscription_workflow ON wmbs_subscription(workflow) %s""" % tablespaceIndex

        self.constraints["01_idx_wmbs_sub_files_acquired"] = \
            """CREATE INDEX idx_wmbs_sub_files_acq_sub ON wmbs_sub_files_acquired(subscription) %s""" % tablespaceIndex

        self.constraints["02_idx_wmbs_sub_files_acquired"] = \
            """CREATE INDEX idx_wmbs_sub_files_acq_file ON wmbs_sub_files_acquired(fileid) %s""" % tablespaceIndex

        self.constraints["01_idx_wmbs_sub_files_available"] = \
            """CREATE INDEX idx_wmbs_sub_files_ava_sub ON wmbs_sub_files_available(subscription) %s""" % tablespaceIndex

        self.constraints["02_idx_wmbs_sub_files_available"] = \
            """CREATE INDEX idx_wmbs_sub_files_ava_file ON wmbs_sub_files_available(fileid) %s""" % tablespaceIndex

        self.constraints["01_idx_wmbs_sub_files_failed"] = \
            """CREATE INDEX idx_wmbs_sub_files_fail_sub ON wmbs_sub_files_failed(subscription) %s""" % tablespaceIndex

        self.constraints["02_idx_wmbs_sub_files_failed"] = \
            """CREATE INDEX idx_wmbs_sub_files_fail_file ON wmbs_sub_files_failed(fileid) %s""" % tablespaceIndex

        self.constraints["01_idx_wmbs_sub_files_complete"] = \
            """CREATE INDEX idx_wmbs_sub_files_comp_sub ON wmbs_sub_files_complete(subscription) %s""" % tablespaceIndex

        self.constraints["02_idx_wmbs_sub_files_complete"] = \
            """CREATE INDEX idx_wmbs_sub_files_comp_file ON wmbs_sub_files_complete(fileid) %s""" % tablespaceIndex

        self.constraints["01_idx_wmbs_sub_jobgroup"] = \
            """CREATE INDEX idx_wmbs_jobgroup_sub ON wmbs_jobgroup(subscription) %s""" % tablespaceIndex

        self.constraints["02_idx_wmbs_sub_jobgroup"] = \
            """CREATE INDEX idx_wmbs_jobgroup_out ON wmbs_jobgroup(output) %s""" % tablespaceIndex

        self.constraints["01_idx_wmbs_job"] = \
            """CREATE INDEX idx_wmbs_job_jobgroup ON wmbs_job(jobgroup) %s""" % tablespaceIndex

        self.constraints["02_idx_wmbs_job"] = \
            """CREATE INDEX idx_wmbs_job_loc ON wmbs_job(location) %s""" % tablespaceIndex

        self.constraints["03_idx_wmbs_job"] = \
            """CREATE INDEX idx_wmbs_job_state ON wmbs_job(state) %s""" % tablespaceIndex

        self.constraints["01_idx_wmbs_job_assoc"] = \
            """CREATE INDEX idx_wmbs_job_assoc_job ON wmbs_job_assoc(job) %s""" % tablespaceIndex

        self.constraints["02_idx_wmbs_job_assoc"] = \
            """CREATE INDEX idx_wmbs_job_assoc_file ON wmbs_job_assoc(fileid) %s""" % tablespaceIndex

        self.constraints["01_idx_wmbs_job_mask"] = \
            """CREATE INDEX idx_wmbs_job_mask_job ON wmbs_job_mask(job) %s""" % tablespaceIndex

        self.constraints["01_idx_wmbs_file_checksums"] = \
            """CREATE INDEX idx_wmbs_file_checksums_type ON wmbs_file_checksums(typeid) %s""" % tablespaceIndex

        self.constraints["01_idx_wmbs_file_checksums"] = \
            """CREATE INDEX idx_wmbs_file_checksums_file ON wmbs_file_checksums(fileid) %s""" % tablespaceIndex

        # The transitions class holds all states and allowed transitions, use
        # that to populate the wmbs_job_state table
        for jobState in Transitions().states():
            jobStateQuery = "INSERT INTO wmbs_job_state (name) VALUES ('%s')" % jobState
            self.inserts["job_state_%s" % jobState] = jobStateQuery

        self.subTypes = [("Processing", 0), ("Merge", 4), ("Harvesting", 5), ("Cleanup", 1),
                         ("LogCollect", 2), ("Skim", 3), ("Production", 0)]
        for pair in self.subTypes:
            subTypeQuery = """INSERT INTO wmbs_sub_types (name, priority)
                                VALUES ('%s', %d)""" % (pair[0], pair[1])
            self.inserts["wmbs_sub_types_%s" % pair[0]] = subTypeQuery

        locationStates = ["Normal", "Down", "Draining", "Aborted"]

        for i in locationStates:
            locationStateQuery = """INSERT INTO wmbs_location_state (name)
                                    VALUES ('%s')""" % i
            self.inserts["wmbs_location_state_%s" % i] = locationStateQuery

        checksumTypes = ['cksum', 'adler32', 'md5']
        for i in checksumTypes:
            checksumTypeQuery = """INSERT INTO wmbs_checksum_type (type) VALUES ('%s') """ % i
            self.inserts["wmbs_checksum_type_%s" % i] = checksumTypeQuery

        return

    def execute(self, conn=None, transaction=None):
        """
        _execute_

        Check to make sure that all required tables have been defined.  If
        everything is in place have the DBCreator make everything.
        """
        for requiredTable in self.requiredTables:
            if requiredTable not in self.create:
                raise WMException("The table '%s' is not defined." % requiredTable, "WMCORE-2")

        DBCreator.execute(self, conn, transaction)
        return True
