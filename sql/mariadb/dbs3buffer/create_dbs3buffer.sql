-- MariaDB tables for WMComponent.DBS3Buffer component

CREATE TABLE dbsbuffer_dataset (
    id              INT          AUTO_INCREMENT,
    path            VARCHAR(500) COLLATE latin1_general_cs NOT NULL,
    processing_ver  INT          NOT NULL,
    acquisition_era VARCHAR(255),
    valid_status    VARCHAR(20),
    global_tag      VARCHAR(255),
    parent          VARCHAR(500),
    prep_id         VARCHAR(255),
    PRIMARY KEY (id),
    CONSTRAINT uq_dbs_dat UNIQUE (path)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE dbsbuffer_dataset_subscription (
    id               INT          AUTO_INCREMENT,
    dataset_id       INT          NOT NULL,
    site             VARCHAR(100) NOT NULL,
    custodial        INT          DEFAULT 0,
    priority         VARCHAR(10)  DEFAULT 'Low',
    subscribed       INT          DEFAULT 0,
    delete_blocks    INT,
    dataset_lifetime INT          DEFAULT 0 NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT uq_dbs_dat_sub UNIQUE (dataset_id, site, custodial, priority),
    FOREIGN KEY (dataset_id) REFERENCES dbsbuffer_dataset(id) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE dbsbuffer_algo (
    id             INT          AUTO_INCREMENT,
    app_name       VARCHAR(100),
    app_ver        VARCHAR(100),
    app_fam        VARCHAR(100),
    pset_hash      VARCHAR(700),
    config_content LONGTEXT,
    in_dbs         INT,
    PRIMARY KEY (id),
    CONSTRAINT uq_dbs_alg UNIQUE (app_name, app_ver, app_fam, pset_hash)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE dbsbuffer_algo_dataset_assoc (
    id         INT AUTO_INCREMENT,
    algo_id    INT NOT NULL,
    dataset_id INT NOT NULL,
    in_dbs     INT DEFAULT 0,
    PRIMARY KEY (id),
    FOREIGN KEY (algo_id) REFERENCES dbsbuffer_algo(id) ON DELETE CASCADE,
    FOREIGN KEY (dataset_id) REFERENCES dbsbuffer_dataset(id) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE dbsbuffer_workflow (
    id                        INT          AUTO_INCREMENT,
    name                      VARCHAR(700),
    task                      VARCHAR(700),
    block_close_max_wait_time INT,
    block_close_max_files     INT,
    block_close_max_events    INT,
    block_close_max_size      BIGINT,
    completed                 INT          DEFAULT 0,
    PRIMARY KEY (id),
    CONSTRAINT uq_dbs_wor UNIQUE (name, task)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE dbsbuffer_file (
    id                   INT          AUTO_INCREMENT,
    lfn                  VARCHAR(500) NOT NULL,
    filesize            BIGINT,
    events              BIGINT UNSIGNED,
    dataset_algo        INT          NOT NULL,
    block_id            INT,
    status              VARCHAR(20),
    in_phedex           INT          DEFAULT 0,
    workflow            INT,
    LastModificationDate INT,
    PRIMARY KEY (id),
    CONSTRAINT uq_dbs_fil UNIQUE (lfn),
    FOREIGN KEY (workflow) REFERENCES dbsbuffer_workflow(id) ON DELETE CASCADE,
    FOREIGN KEY (dataset_algo) REFERENCES dbsbuffer_algo_dataset_assoc(id) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE dbsbuffer_file_parent (
    child  INT NOT NULL,
    parent INT NOT NULL,
    PRIMARY KEY (child, parent),
    FOREIGN KEY (child) REFERENCES dbsbuffer_file(id) ON DELETE CASCADE,
    FOREIGN KEY (parent) REFERENCES dbsbuffer_file(id) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE dbsbuffer_file_runlumi_map (
    filename   INT NOT NULL,
    run        INT NOT NULL,
    lumi       INT NOT NULL,
    num_events BIGINT UNSIGNED,
    FOREIGN KEY (filename) REFERENCES dbsbuffer_file(id) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE dbsbuffer_location (
    id   INT          AUTO_INCREMENT,
    pnn  VARCHAR(255) NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT uq_dbs_loc UNIQUE (pnn)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE dbsbuffer_file_location (
    filename INT NOT NULL,
    location INT NOT NULL,
    PRIMARY KEY (filename, location),
    FOREIGN KEY (filename) REFERENCES dbsbuffer_file(id) ON DELETE CASCADE,
    FOREIGN KEY (location) REFERENCES dbsbuffer_location(id) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE dbsbuffer_block (
    id          INT          AUTO_INCREMENT,
    dataset_id  INT          NOT NULL,
    blockname   VARCHAR(250) NOT NULL,
    location    INT          NOT NULL,
    create_time INT,
    status      VARCHAR(20),
    deleted     INT          DEFAULT 0,
    rule_id     VARCHAR(40)  NOT NULL DEFAULT '0',
    PRIMARY KEY (id),
    CONSTRAINT uq_dbs_blo UNIQUE (blockname, location),
    FOREIGN KEY (dataset_id) REFERENCES dbsbuffer_dataset(id) ON DELETE CASCADE,
    FOREIGN KEY (location) REFERENCES dbsbuffer_location(id) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE dbsbuffer_checksum_type (
    id   INT          AUTO_INCREMENT,
    type VARCHAR(255),
    PRIMARY KEY (id)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE dbsbuffer_file_checksums (
    fileid INT    NOT NULL,
    typeid INT    NOT NULL,
    cksum  VARCHAR(100),
    PRIMARY KEY (fileid, typeid),
    FOREIGN KEY (fileid) REFERENCES dbsbuffer_file(id) ON DELETE CASCADE,
    FOREIGN KEY (typeid) REFERENCES dbsbuffer_checksum_type(id) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

-- Indexes
CREATE INDEX idx_dbs_fil_che_1 ON dbsbuffer_file_checksums (typeid);
CREATE INDEX idx_dbs_fil_che_2 ON dbsbuffer_file_checksums (fileid);
CREATE INDEX idx_dbs_dat_sub_1 ON dbsbuffer_dataset_subscription (dataset_id);
CREATE INDEX idx_dbs_fil_par_1 ON dbsbuffer_file_parent (child);
CREATE INDEX idx_dbs_fil_par_2 ON dbsbuffer_file_parent (parent);
CREATE INDEX idx_dbs_blo_1 ON dbsbuffer_block (location);
CREATE INDEX idx_dbs_blo_2 ON dbsbuffer_block (dataset_id);
CREATE INDEX idx_dbs_alg_ass_1 ON dbsbuffer_algo_dataset_assoc (dataset_id);
CREATE INDEX idx_dbs_alg_ass_2 ON dbsbuffer_algo_dataset_assoc (algo_id);
CREATE INDEX idx_dbs_fil_run_1 ON dbsbuffer_file_runlumi_map (filename);
CREATE INDEX idx_dbs_fil_loc_1 ON dbsbuffer_file_location (location);
CREATE INDEX idx_dbs_fil_loc_2 ON dbsbuffer_file_location (filename);
CREATE INDEX idx_dbs_fil_1 ON dbsbuffer_file (workflow);

-- Initial data
INSERT INTO dbsbuffer_checksum_type (type) VALUES ('cksum');
INSERT INTO dbsbuffer_checksum_type (type) VALUES ('adler32');
INSERT INTO dbsbuffer_checksum_type (type) VALUES ('md5'); 