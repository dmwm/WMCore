-- Table creation statements for MariaDB

-- Independent tables (no dependencies)
CREATE TABLE wmbs_fileset (
    id          INTEGER        PRIMARY KEY AUTO_INCREMENT,
    name        VARCHAR(1250)  NOT NULL,
    open        TINYINT(1)     NOT NULL DEFAULT 0,
    last_update INTEGER        NOT NULL,
    CONSTRAINT wmbs_fileset_unique UNIQUE (name)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE wmbs_location_state (
    id   INTEGER       PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100)  NOT NULL
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE wmbs_pnns (
    id  INTEGER      PRIMARY KEY AUTO_INCREMENT,
    pnn VARCHAR(255),
    CONSTRAINT wmbs_pnns_unique UNIQUE (pnn)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE wmbs_users (
    id          INTEGER      PRIMARY KEY AUTO_INCREMENT,
    cert_dn     VARCHAR(255) NOT NULL,
    name_hn     VARCHAR(255),
    owner       VARCHAR(255),
    grp         VARCHAR(255),
    group_name  VARCHAR(255),
    role_name   VARCHAR(255),
    CONSTRAINT wmbs_users_unique UNIQUE (cert_dn, group_name, role_name)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE wmbs_job_state (
    id   INTEGER      PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE wmbs_checksum_type (
    id   INTEGER      PRIMARY KEY AUTO_INCREMENT,
    type VARCHAR(255) NOT NULL
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE wmbs_sub_types (
    id       INTEGER      PRIMARY KEY AUTO_INCREMENT,
    name     VARCHAR(255) NOT NULL,
    priority INTEGER      DEFAULT 0,
    CONSTRAINT wmbs_sub_types_unique UNIQUE (name)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

-- Tables with single dependencies
CREATE TABLE wmbs_file_details (
    id           INTEGER        PRIMARY KEY AUTO_INCREMENT,
    lfn          VARCHAR(1250)  NOT NULL,
    filesize     BIGINT,
    events       BIGINT UNSIGNED,
    first_event  BIGINT UNSIGNED NOT NULL DEFAULT 0,
    merged       TINYINT(1)     NOT NULL DEFAULT 0,
    CONSTRAINT wmbs_file_details_unique UNIQUE (lfn)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE wmbs_location (
    id            INTEGER      PRIMARY KEY AUTO_INCREMENT,
    site_name     VARCHAR(255) NOT NULL,
    state         INTEGER      NOT NULL,
    cms_name      VARCHAR(255),
    ce_name       VARCHAR(255),
    running_slots INTEGER,
    pending_slots INTEGER,
    plugin        VARCHAR(255),
    state_time    INTEGER      DEFAULT 0,
    CONSTRAINT wmbs_location_unique UNIQUE (site_name),
    CONSTRAINT wmbs_location_state_fk FOREIGN KEY (state) 
        REFERENCES wmbs_location_state(id)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE wmbs_workflow (
    id           INTEGER       PRIMARY KEY AUTO_INCREMENT,
    spec         VARCHAR(700)  NOT NULL,
    name         VARCHAR(255)  NOT NULL,
    task         VARCHAR(1250) NOT NULL,
    type         VARCHAR(255),
    owner        INTEGER       NOT NULL,
    alt_fs_close TINYINT(1)   NOT NULL,
    injected     TINYINT(1)   DEFAULT 0,
    priority     INTEGER       DEFAULT 0,
    CONSTRAINT wmbs_workflow_unique UNIQUE (name, spec, task),
    CONSTRAINT wmbs_workflow_fk_users FOREIGN KEY (owner) 
        REFERENCES wmbs_users(id) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

-- Tables with multiple dependencies
CREATE TABLE wmbs_fileset_files (
    fileid      INTEGER NOT NULL,
    fileset     INTEGER NOT NULL,
    insert_time INTEGER NOT NULL,
    CONSTRAINT wmbs_fileset_files_pk UNIQUE (fileid, fileset),
    CONSTRAINT wmbs_fileset_files_fk FOREIGN KEY (fileset) 
        REFERENCES wmbs_fileset(id) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE wmbs_file_parent (
    child  INTEGER NOT NULL,
    parent INTEGER NOT NULL,
    CONSTRAINT wmbs_file_parent_pk UNIQUE (child, parent),
    CONSTRAINT wmbs_file_parent_child_fk FOREIGN KEY (child) 
        REFERENCES wmbs_file_details(id) ON DELETE CASCADE,
    CONSTRAINT wmbs_file_parent_parent_fk FOREIGN KEY (parent) 
        REFERENCES wmbs_file_details(id)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE wmbs_file_runlumi_map (
    fileid     INTEGER        NOT NULL,
    run        INTEGER        NOT NULL,
    lumi       INTEGER        NOT NULL,
    num_events BIGINT UNSIGNED,
    CONSTRAINT wmbs_file_runlumi_pk PRIMARY KEY (fileid, run, lumi),
    CONSTRAINT wmbs_file_runlumi_fk FOREIGN KEY (fileid) 
        REFERENCES wmbs_file_details(id) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

-- Tables with multiple dependencies
CREATE TABLE wmbs_location_pnns (
    location INTEGER,
    pnn      INTEGER,
    CONSTRAINT wmbs_location_pnns_pk UNIQUE (location, pnn),
    CONSTRAINT wmbs_location_pnns_loc_fk FOREIGN KEY (location) 
        REFERENCES wmbs_location(id) ON DELETE CASCADE,
    CONSTRAINT wmbs_location_pnns_pnn_fk FOREIGN KEY (pnn) 
        REFERENCES wmbs_pnns(id) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE wmbs_file_location (
    fileid   INTEGER NOT NULL,
    pnn      INTEGER NOT NULL,
    CONSTRAINT wmbs_file_location_pk PRIMARY KEY (fileid, pnn),
    CONSTRAINT wmbs_file_location_fk_file FOREIGN KEY (fileid) 
        REFERENCES wmbs_file_details(id) ON DELETE CASCADE,
    CONSTRAINT wmbs_file_location_fk_pnn FOREIGN KEY (pnn)
        REFERENCES wmbs_pnns(id) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE wmbs_workflow_output (
    workflow_id           INTEGER      NOT NULL,
    output_identifier    VARCHAR(255) NOT NULL,
    output_fileset       INTEGER      NOT NULL,
    merged_output_fileset INTEGER,
    CONSTRAINT wmbs_workflow_output_pk PRIMARY KEY (workflow_id, output_identifier, output_fileset),
    CONSTRAINT wmbs_workflow_output_fk1 FOREIGN KEY (workflow_id) 
        REFERENCES wmbs_workflow(id) ON DELETE CASCADE,
    CONSTRAINT wmbs_workflow_output_fk2 FOREIGN KEY (output_fileset) 
        REFERENCES wmbs_fileset(id) ON DELETE CASCADE,
    CONSTRAINT wmbs_workflow_output_fk3 FOREIGN KEY (merged_output_fileset) 
        REFERENCES wmbs_fileset(id) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE wmbs_subscription (
    id           INTEGER      PRIMARY KEY AUTO_INCREMENT,
    fileset      INTEGER      NOT NULL,
    workflow     INTEGER      NOT NULL,
    split_algo   VARCHAR(255) NOT NULL,
    subtype      INTEGER      NOT NULL,
    last_update  INTEGER      NOT NULL,
    finished     TINYINT(1)   DEFAULT 0,
    CONSTRAINT wmbs_subscription_fk_fileset FOREIGN KEY (fileset) 
        REFERENCES wmbs_fileset(id) ON DELETE CASCADE,
    CONSTRAINT wmbs_subscription_fk_workflow FOREIGN KEY (workflow) 
        REFERENCES wmbs_workflow(id) ON DELETE CASCADE,
    CONSTRAINT wmbs_subscription_fk_subtype FOREIGN KEY (subtype) 
        REFERENCES wmbs_sub_types(id) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE wmbs_subscription_validation (
    subscription_id INTEGER NOT NULL,
    location_id    INTEGER NOT NULL,
    valid         TINYINT(1),
    CONSTRAINT wmbs_sub_val_pk UNIQUE (subscription_id, location_id),
    CONSTRAINT wmbs_sub_val_fk1 FOREIGN KEY (subscription_id) 
        REFERENCES wmbs_subscription(id) ON DELETE CASCADE,
    CONSTRAINT wmbs_sub_val_fk2 FOREIGN KEY (location_id) 
        REFERENCES wmbs_location(id) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE wmbs_sub_files_available (
    subscription INTEGER NOT NULL,
    fileid      INTEGER NOT NULL,
    CONSTRAINT wmbs_sub_files_available_pk PRIMARY KEY (subscription, fileid),
    CONSTRAINT wmbs_sub_files_available_fk_sub FOREIGN KEY (subscription) 
        REFERENCES wmbs_subscription(id) ON DELETE CASCADE,
    CONSTRAINT wmbs_sub_files_available_fk_file FOREIGN KEY (fileid) 
        REFERENCES wmbs_file_details(id) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE wmbs_sub_files_acquired (
    subscription INTEGER NOT NULL,
    fileid      INTEGER NOT NULL,
    CONSTRAINT wmbs_sub_files_acquired_pk PRIMARY KEY (subscription, fileid),
    CONSTRAINT wmbs_sub_files_acquired_fk_sub FOREIGN KEY (subscription) 
        REFERENCES wmbs_subscription(id) ON DELETE CASCADE,
    CONSTRAINT wmbs_sub_files_acquired_fk_file FOREIGN KEY (fileid) 
        REFERENCES wmbs_file_details(id) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE wmbs_sub_files_failed (
    subscription INTEGER NOT NULL,
    fileid      INTEGER NOT NULL,
    CONSTRAINT wmbs_sub_files_failed_pk PRIMARY KEY (subscription, fileid),
    CONSTRAINT wmbs_sub_files_failed_fk_sub FOREIGN KEY (subscription) 
        REFERENCES wmbs_subscription(id) ON DELETE CASCADE,
    CONSTRAINT wmbs_sub_files_failed_fk_file FOREIGN KEY (fileid) 
        REFERENCES wmbs_file_details(id) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE wmbs_sub_files_complete (
    subscription INTEGER NOT NULL,
    fileid      INTEGER NOT NULL,
    CONSTRAINT wmbs_sub_files_complete_pk PRIMARY KEY (subscription, fileid),
    CONSTRAINT wmbs_sub_files_complete_fk_sub FOREIGN KEY (subscription) 
        REFERENCES wmbs_subscription(id) ON DELETE CASCADE,
    CONSTRAINT wmbs_sub_files_complete_fk_file FOREIGN KEY (fileid) 
        REFERENCES wmbs_file_details(id) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE wmbs_jobgroup (
    id           INTEGER      PRIMARY KEY AUTO_INCREMENT,
    subscription INTEGER      NOT NULL,
    guid         VARCHAR(255),
    output       INTEGER,
    last_update  INTEGER      NOT NULL,
    location     INTEGER,
    CONSTRAINT wmbs_jobgroup_unique UNIQUE (guid),
    CONSTRAINT wmbs_jobgroup_fk_sub FOREIGN KEY (subscription) 
        REFERENCES wmbs_subscription(id) ON DELETE CASCADE,
    CONSTRAINT wmbs_jobgroup_fk_output FOREIGN KEY (output) 
        REFERENCES wmbs_fileset(id) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE wmbs_job (
    id           INTEGER       PRIMARY KEY AUTO_INCREMENT,
    jobgroup     INTEGER       NOT NULL,
    name         VARCHAR(255),
    state        INTEGER       NOT NULL,
    state_time   INTEGER       NOT NULL,
    retry_count  INTEGER       DEFAULT 0,
    couch_record VARCHAR(255),
    location     INTEGER,
    outcome      INTEGER       DEFAULT 0,
    cache_dir    VARCHAR(1250) DEFAULT NULL,
    fwjr_path    VARCHAR(1250),
    CONSTRAINT wmbs_job_unique UNIQUE (name, cache_dir, fwjr_path),
    CONSTRAINT wmbs_job_fk_group FOREIGN KEY (jobgroup) 
        REFERENCES wmbs_jobgroup(id) ON DELETE CASCADE,
    CONSTRAINT wmbs_job_fk_state FOREIGN KEY (state) 
        REFERENCES wmbs_job_state(id),
    CONSTRAINT wmbs_job_fk_location FOREIGN KEY (location) 
        REFERENCES wmbs_location(id)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE wmbs_job_assoc (
    job    INTEGER NOT NULL,
    fileid INTEGER NOT NULL,
    CONSTRAINT wmbs_job_assoc_pk PRIMARY KEY (job, fileid),
    CONSTRAINT wmbs_job_assoc_fk1 FOREIGN KEY (job) 
        REFERENCES wmbs_job(id) ON DELETE CASCADE,
    CONSTRAINT wmbs_job_assoc_fk2 FOREIGN KEY (fileid) 
        REFERENCES wmbs_file_details(id) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE wmbs_job_mask (
    job           INTEGER NOT NULL,
    FirstEvent    BIGINT UNSIGNED,
    LastEvent     BIGINT UNSIGNED,
    FirstLumi     INTEGER,
    LastLumi      INTEGER,
    FirstRun      INTEGER,
    LastRun       INTEGER,
    inclusivemask TINYINT(1) DEFAULT 1,
    CONSTRAINT wmbs_job_mask_fk FOREIGN KEY (job) 
        REFERENCES wmbs_job(id) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE wmbs_file_checksums (
    fileid INTEGER      NOT NULL,
    typeid INTEGER      NOT NULL,
    cksum  VARCHAR(100) NOT NULL,
    CONSTRAINT wmbs_file_checksums_pk UNIQUE (fileid, typeid),
    CONSTRAINT wmbs_file_checksums_type_fk FOREIGN KEY (typeid)
        REFERENCES wmbs_checksum_type(id) ON DELETE CASCADE,
    CONSTRAINT wmbs_file_checksums_file_fk FOREIGN KEY (fileid)
        REFERENCES wmbs_file_details(id) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE wmbs_workunit (
    id              INTEGER PRIMARY KEY AUTO_INCREMENT,
    taskid          INTEGER NOT NULL,
    retry_count     INTEGER DEFAULT 0,
    last_unit_count INTEGER NOT NULL,
    last_submit_time INTEGER NOT NULL,
    status          TINYINT(1) DEFAULT 0,
    CONSTRAINT wmbs_workunit_fk_task FOREIGN KEY (taskid) 
        REFERENCES wmbs_workflow(id) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE wmbs_job_workunit_assoc (
    job      INTEGER NOT NULL,
    workunit INTEGER NOT NULL,
    CONSTRAINT wmbs_job_wu_assoc_pk PRIMARY KEY (job, workunit),
    CONSTRAINT wmbs_job_wu_assoc_fk1 FOREIGN KEY (job) 
        REFERENCES wmbs_job(id) ON DELETE CASCADE,
    CONSTRAINT wmbs_job_wu_assoc_fk2 FOREIGN KEY (workunit) 
        REFERENCES wmbs_workunit(id) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE wmbs_frl_workunit_assoc (
    workunit   INTEGER NOT NULL,
    firstevent INTEGER DEFAULT 0,
    lastevent  INTEGER DEFAULT 0,
    fileid     INTEGER NOT NULL,
    run        INTEGER NOT NULL,
    lumi       INTEGER NOT NULL,
    CONSTRAINT wmbs_frl_wu_assoc_pk PRIMARY KEY (workunit, fileid, run, lumi),
    CONSTRAINT wmbs_frl_wu_assoc_fk1 FOREIGN KEY (workunit) 
        REFERENCES wmbs_workunit(id) ON DELETE CASCADE,
    CONSTRAINT wmbs_frl_wu_assoc_fk2 FOREIGN KEY (fileid, run, lumi) 
        REFERENCES wmbs_file_runlumi_map(fileid, run, lumi) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;
