-- MariaDB tables for WMCore.Agent.Database component

CREATE TABLE wm_init (
    init_param VARCHAR(100) NOT NULL UNIQUE,
    init_value VARCHAR(100) NOT NULL
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE wm_components (
    id               INT(11)      NOT NULL AUTO_INCREMENT,
    name             VARCHAR(255) NOT NULL,
    pid              INT(11)      NOT NULL,
    update_threshold INT(11)      NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY wm_components_uniq (name)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE wm_workers (
    component_id    INT(11)      NOT NULL,
    name           VARCHAR(255)  NOT NULL,
    last_updated   INT(11)      NOT NULL,
    state          VARCHAR(255),
    pid            INT(11),
    poll_interval  INT(11)      NOT NULL,
    last_error     INT(11),
    cycle_time     FLOAT        DEFAULT 0 NOT NULL,
    outcome        VARCHAR(1000),
    error_message  VARCHAR(1000),
    UNIQUE KEY wm_workers_uniq (name),
    CONSTRAINT wm_workers_fk_comp FOREIGN KEY (component_id) 
        REFERENCES wm_components(id)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC; 