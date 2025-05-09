-- MariaDB tables for WMCore.BossAir component

CREATE TABLE bl_status (
    id            INT          AUTO_INCREMENT,
    name          VARCHAR(255),
    PRIMARY KEY (id),
    UNIQUE (name)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE bl_runjob (
    id            INT          AUTO_INCREMENT,
    wmbs_id       INT,
    grid_id       VARCHAR(255),
    bulk_id       VARCHAR(255),
    status        CHAR(1)      DEFAULT '1',
    sched_status  INT          NOT NULL,
    retry_count   INT,
    status_time   INT,
    location      INT,
    user_id       INT,
    PRIMARY KEY (id),
    FOREIGN KEY (wmbs_id) REFERENCES wmbs_job(id) ON DELETE CASCADE,
    FOREIGN KEY (sched_status) REFERENCES bl_status(id),
    FOREIGN KEY (user_id) REFERENCES wmbs_users(id) ON DELETE CASCADE,
    FOREIGN KEY (location) REFERENCES wmbs_location(id) ON DELETE CASCADE,
    UNIQUE (retry_count, wmbs_id)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

-- Indexes
CREATE INDEX idx_bl_runjob_wmbs ON bl_runjob(wmbs_id);
CREATE INDEX idx_bl_runjob_status ON bl_runjob(sched_status);
CREATE INDEX idx_bl_runjob_users ON bl_runjob(user_id);
CREATE INDEX idx_bl_runjob_location ON bl_runjob(location); 