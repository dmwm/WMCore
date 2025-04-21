-- MariaDB tables for WMCore.ResourceControl component

CREATE TABLE rc_threshold (
    site_id       INT    NOT NULL,
    sub_type_id   INT    NOT NULL,
    pending_slots INT    NOT NULL,
    max_slots     INT    NOT NULL,
    FOREIGN KEY (site_id) REFERENCES wmbs_location(id) ON DELETE CASCADE,
    FOREIGN KEY (sub_type_id) REFERENCES wmbs_sub_types(id) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC; 